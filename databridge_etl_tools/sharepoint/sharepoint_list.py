import csv
from collections.abc import Iterator
from itertools import chain

import boto3
from espy.list import SharePointList
from espy.models.models import APICredentials

from ..models.models import SharePointListArgs


def _create_sp_list(site_name: str,
                    list_name: str,
                    hostname: str,
                    creds: APICredentials) -> SharePointList:
    """
    Instantiates an EsPy SharePointList object.

    Args:
        site_name: The name of the SharePoint site
        list_name: The name of the list
        hostname: The hostname of the SharePoint site
        creds: An APICredentials object

    Returns:
        SharePointList: A SharePointList object
    """

    return SharePointList.setup(site_name=site_name,
                                list_name=list_name,
                                hostname=hostname,
                                creds=creds)

def _save_to_csv(rows: Iterator[dict], csv_path: str) -> None:
    """
    Saves an iterator of SharePoint list rows to a csv at the provided path.

    Args:
        rows: An iterator of dicts containing SharePoint list rows
        csv_path: The path to save to

    Returns: None
    """
    try:
        first_row = next(rows)
        headers = list(first_row.keys())

    except StopIteration:
        raise ValueError("No rows present in the SharePoint list.") from None

    with open(csv_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, headers)
        writer.writeheader()
        full_stream = chain([first_row], rows)
        writer.writerows(full_stream)

def _load_to_s3(s3_bucket: str,
               s3_key: str,
               csv_path: str) -> None:
    """
    Loads a saved CSV file to s3.

    Args:
        s3_bucket: The name of the s3_bucket
        s3_key: The s3_key
        csv_path: The path of the CSV to load

    Returns: None.
    """

    s3 = boto3.resource('s3')
    with open(csv_path, mode="rb") as f:
        s3.Object(s3_bucket, s3_key).put(Body=f)

def extract_func(list_args: SharePointListArgs):

    creds: APICredentials = {
        'tenant_id': list_args.graphapi_tenant_id,
        'client_id': list_args.graphapi_application_id,
        'client_secret': list_args.graphapi_secret_value
    }

    sp_list = _create_sp_list(
        site_name=list_args.site_name,
        list_name=list_args.list_name,
        hostname=list_args.hostname,
        creds=creds
    )

    content = sp_list.list_rows()

    path = '/tmp/output.csv' if not list_args.csv_path else list_args.csv_path

    try:
        _save_to_csv(content, path)

    except ValueError as ve:
        raise ValueError(f"An error occurred when trying to download {list_args.list_name} from {list_args.site_name}: {ve}")

    if list_args.debug:
        print(f"Content written to csv at {path}")

    if list_args.s3_bucket and list_args.s3_key:
        _load_to_s3(list_args.s3_bucket, list_args.s3_key, path)
        
        if list_args.debug:
            print(f"Content successfully loaded to {list_args.s3_bucket}/{list_args.s3_key}")