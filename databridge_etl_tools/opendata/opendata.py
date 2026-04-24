import csv
import gzip
import logging
import os
import re
import shutil
import sys

import boto3
import botocore
import petl as etl
import psycopg2
import psycopg2.extras
import pyproj
import pytz
from shapely import wkt


class OpenData:
    "Takes a CSV from our S3 bucket, transforms points to lat/lng, and uploads it to the opendata bucket, as well as a zipped version"

    _pg_cursor = None
    _logger = None

    def __init__(
        self,
        table_name,
        table_schema,
        s3_bucket,
        s3_key,
        opendata_bucket,
        libpq_conn_string=None,
    ):
        self.table_name = table_name
        self.table_schema = table_schema
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.opendata_bucket = opendata_bucket
        self.libpq_conn_string = libpq_conn_string

    @property
    def logger(self):
        if self._logger is None:
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.INFO)
            sh = logging.StreamHandler(sys.stdout)
            logger.addHandler(sh)
            self._logger = logger
        return self._logger

    @property
    def pg_cursor(self):
        if self._pg_cursor is None:
            self.conn = psycopg2.connect(self.libpq_conn_string, connect_timeout=6)
            assert self.conn.closed == 0
            self.conn.autocommit = False
            self.conn.set_session(autocommit=False)
            self._pg_cursor = self.conn.cursor()
        return self._pg_cursor

    @property
    def csv_path(self):
        return "/tmp/" + self.s3_key.split("/")[2]

    def download_csv_from_s3(self):
        self.logger.info("Fetching csv s3://{}/{}".format(self.s3_bucket, self.s3_key))

        s3 = boto3.resource("s3")
        try:
            s3.Object(self.s3_bucket, self.s3_key).download_file(self.csv_path)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                raise AssertionError(
                    f"CSV file doesnt appear to exist in S3! key: {self.s3_key}"
                )
            else:
                raise e

    def compress_csv(self, filename):
        compress_filename = filename + ".gz"
        level = 7
        self.logger.info(
            f"Gzipping csv file into {compress_filename} with gzip, compression level: {level}"
        )
        with open(filename, "rb") as f_in:
            with gzip.open(
                filename=compress_filename, mode="wb", compresslevel=level
            ) as f_out:
                shutil.copyfileobj(f_in, f_out)
        self.logger.info("Gzipped.")

    def transform_and_upload_data(self):
        rows = etl.fromcsv(self.csv_path, encoding="utf-8")

        # make header (field names) lowercase
        header = rows[0]
        # Get header row
        header_fmt = [h.lower() for h in header]

        # Determine if geometric first
        geometric = False
        geom_field = None
        geom_type = None
        srid = None
        stmt = f"""
        SELECT column_name,data_type
        FROM information_schema.columns
        WHERE
            table_schema = 'viewer' AND
            table_name = 'opa__assessments' AND
            column_name = 'shape';
        """
        self.pg_cursor.execute(stmt)
        geometric_field = self.pg_cursor.fetchall()
        if geometric_field:
            print(f"DEBUG: {geometric_field}")
            if (
                geometric_field[0][0] == "shape"
                and geometric_field[0][1] == "USER-DEFINED"
            ):
                geometric = True

        if geometric:
            # First, let's try to find the SRID
            # Let's see if we can get it our SRID from the crazy SDE xml definition that lives in this sde system table
            stmt = f"SELECT definition FROM sde.gdb_items WHERE name = 'databridge.{self.table_schema}.{self.table_name}'"
            print(f"Running stmt: {stmt}")
            self.pg_cursor.execute(stmt)
            xml_def = self.pg_cursor.fetchone()
            if xml_def:
                xml_def = xml_def[0]
                if "<LatestWKID>" in xml_def:
                    match = re.search(r"<LatestWKID>.*</LatestWKID>", xml_def)
                    if match:
                        srid = (
                            match.group()
                            .replace("<LatestWKID>", "")
                            .replace("</LatestWKID>", "")
                        )
            # If we can't find it, see if we put it in the CSV in the shape values, like so: SRID=2272; POINT ( 1234, 5678 )
            if not srid:
                # Find non-null shape column to determine the SRID
                # Pull the first 1000 rows in case we have like, 5 million rows so this won't take forever.
                # and what kind of dataset would have empty shapes for the first 1000 rows?
                thousand_rows = etl.head(rows, 1000)
                shapes = etl.cut(thousand_rows, "shape")
                for row in shapes:
                    if row[0]:
                        # Try to regex for the SRID
                        match = re.search(r"[=].*[;]", row[0])
                        if not match:
                            match = re.search(r"=.*;", row[0])
                        if not match:
                            continue
                        else:
                            break
                if srid:
                    # Strip characters used to regex from the matched string
                    srid = match.group().replace("=", "").replace(";", "")

            assert srid
            print(f"SRID detected as: {srid}")

            # Get geom type from DB2. First try with a PostGIS table.
            stmt = f"""
            SELECT type FROM geometry_columns
            WHERE f_table_schema = '{self.table_schema}'
            AND f_table_name = '{self.table_name}'
            AND f_geometry_column = 'shape';
            """
            print(f"Running stmt: {stmt}")
            self.pg_cursor.execute(stmt)
            geom_type = self.pg_cursor.fetchone()
            if geom_type:
                geom_type = geom_type[0]
            # If the geom_type is None or if it gave us back a generic "GEOMETRY"
            # Then try extracting from the SDE XML definition.
            if (not geom_type) or (geom_type == "GEOMETRY"):
                stmt = f"SELECT definition FROM sde.gdb_items WHERE name = 'databridge.{self.table_schema}.{self.table_name}'"
                print(f"Running stmt: {stmt}")
                self.pg_cursor.execute(stmt)
                geom_type = self.pg_cursor.fetchone()[0]
            if "<ShapeType>esriGeometryPoint</ShapeType>" in geom_type:
                geom_type = "point"
            elif "<ShapeType>esriGeometryPolygon</ShapeType>" in geom_type:
                geom_type = "polygon"
            elif "<ShapeType>esriGeometryPolyline</ShapeType>" in geom_type:
                geom_type = "line"
            else:
                raise AssertionError("Could not determine geometry type from database")

            # Get geom field
            stmt = f"""
                    SELECT f_geometry_column FROM public.geometry_columns
                    WHERE f_table_schema = '{self.table_schema}'
                    AND f_table_name = '{self.table_name}'
                    """
            print(f"Running stmt: {stmt}")
            self.pg_cursor.execute(stmt)
            geom_field = self.pg_cursor.fetchone()[0]
            assert geom_field

        # IF geom_type is a point, extract lat/lon and then remove shape field.
        # All other geom types aren't handled in csv's for open data.
        # Per Alex, the way most people expect the data to be in is lat/long, but lines and polyons are okay as WKT.
        if geom_type == "point":
            lon_variations = [
                "lon",
                "lng",
                "long",
                "longitude",
                "x",
                "x_coord",
                "x_cord",
                "x_coordinate",
                "coord_x",
                "cord_x",
                "coordinate_x",
            ]
            lat_variations = [
                "lat",
                "latitude",
                "y",
                "y_coord",
                "y_cord",
                "y_coordinate",
                "coord_y",
                "cord_y",
                "coordinate_y",
            ]
            # Remove pre-existing coordinate fields:
            header_set = set(header_fmt)
            # Get existing fields that match our lat and lon lists above
            lon_and_lat_variations = lon_variations + lat_variations
            lon_and_lat_variations_set = set(lon_and_lat_variations)
            lons_and_lats_in_header = header_set.intersection(
                lon_and_lat_variations_set
            )
            for field in list(lons_and_lats_in_header):
                # Use petl to cut out the fields and header at the same time.
                rows = rows.cutout(field)

            # map bad SRIDs that need to be corrected to 2272
            bad_srid_map = {
                300001: 2272,
                300003: 2272,
                300046: 2272,
                300006: 2272,
                300010: 2272,
                300008: 2272,
                300004: 2272,
                300007: 2272,
                300067: 2272,
                300100: 2272,
                300101: 2272,
                300084: 3857,
                300073: 4326,
                300042: 4326,
                300090: 4269,
                300091: 4326,
                300092: 4326,
                300042: 4326,
                300086: 6565,
                300087: 6565,
                300093: 2272,
            }

            # Always to 4326 for opendata uploads
            to_srid = 4326
            from_srid = bad_srid_map.get(srid, srid)
            self.logger.info("from_srid: {}, to_srid: {}".format(from_srid, to_srid))

            # Remove SRID= from shape field by splitting on semicolon, example shape:
            # 'SRID=2272;POINT ( 2674485.16144665 240563.70777297)'
            rows = rows.convert("shape", lambda s: s.split(";")[1] if s else "")

            # project the dataset from the input srid to the correct srid that we want.
            # Purposefully defined outside of function to speed up operations.
            transformer = pyproj.Transformer.from_crs(
                "epsg:{}".format(from_srid), "epsg:{}".format(to_srid), always_xy=True
            )

            def project_shape(shape, from_srid, to_srid, transformer):
                if from_srid == to_srid:
                    return shape
                pt = wkt.loads(shape)
                x, y = transformer.transform(pt.x, pt.y)
                if (not x) or (not y):
                    return ""
                return f"POINT({x} {y})"

            # project_shape will properly convert the coordinate system
            rows = rows.convert(
                "shape",
                lambda s: (
                    project_shape(s, from_srid, to_srid, transformer) if s else ""
                ),
            )

            # Extract lat/lng from shape TODO use shapely methods instead:
            rows_fmt = rows.addfield(
                "lat",
                lambda a: (
                    float(
                        a["shape"]
                        .replace("POINT", "")
                        .split(" ")[0]
                        .replace("(", "")
                        .replace(")", "")
                    )
                    if a["shape"]
                    else ""
                ),
            ).addfield(
                "lng",
                lambda a: (
                    float(
                        a["shape"]
                        .replace("POINT", "")
                        .split(" ")[1]
                        .replace("(", "")
                        .replace(")", "")
                    )
                    if a["shape"]
                    else ""
                ),
            )
        else:
            rows_fmt = rows

        # If there's no geometric field, geom_field will evalaute false as an empty string
        if geom_field:
            rows_fmt = rows_fmt.cutout("{}".format(geom_field.lower()))

        # Dump to the csv file
        finished_csv_path = self.csv_path.replace(".csv", "_final.csv")
        rows_fmt.tocsv(finished_csv_path, encoding="utf-8")

        # Swap out original csv with final csv.
        os.remove(self.csv_path)
        os.rename(finished_csv_path, self.csv_path)

        print("CSV successfully transformed.")

        file_name = self.s3_key.split("/")[2]

        s3 = boto3.resource("s3")
        s3.Object(self.opendata_bucket, file_name).upload_file(self.csv_path)
        print(
            f"Uploaded {self.csv_path} to bucket {self.opendata_bucket} as {file_name}"
        )

        self.compress_csv(self.csv_path)
        s3.Object(self.opendata_bucket, file_name + ".gz").upload_file(
            self.csv_path + ".gz"
        )
        # Also set as gzip for now, until I can have kistine change the filenames to the more proper ".gz"
        s3.Object(self.opendata_bucket, file_name + ".gzip").upload_file(
            self.csv_path + ".gz"
        )
        print(
            f"Uploaded {self.csv_path + '.gz'} to bucket {self.opendata_bucket} as {file_name + '.gz'}"
        )

    def run(self):
        self.download_csv_from_s3()
        self.transform_and_upload_data()
