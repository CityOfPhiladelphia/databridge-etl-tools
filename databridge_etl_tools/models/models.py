from dataclasses import dataclass

from pydantic import SecretStr


@dataclass
class SharePointListArgs:
    graphapi_tenant_id: SecretStr
    graphapi_application_id: SecretStr
    graphapi_secret_value: SecretStr
    hostname: str
    site_name: str
    list_name: str
    s3_bucket: str | None = None
    s3_key: str | None = None
    csv_path: str | None = None
    debug: bool = False