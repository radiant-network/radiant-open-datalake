"""
Holds configuration values and global constants for the project.

Note: Some Airflow or environment variables may be missing. This is intentional,
as certain operators only require a subset of the configuration and do not need
all variables to be present.
"""

# Importing from airflow.models instead of airflow.sdk to avoid test initialization issues.
# This is fixed in newer Airflow versions, so you can switch back to airflow.sdk if we upgrade.
import os

from airflow.models import Variable

# S3 settings
environment = Variable.get("environment", "dev")
s3_conn_id = "opendatalake_s3"

raw_datalake_bucket = os.getenv("OPENDATALAKE_RAW_BUCKET", f"opendatalake-{environment}")

# Root under the bucket where raw source files land. (passed to Spark as --raw-storage).
# Keep in sync with the Spark raw_storage root.
_raw_landing_root = os.getenv("OPENDATALAKE_RAW_LANDING_ROOT", "raw/landing")


def raw_landing_prefix(source: str, version: str) -> str:
    """S3 key prefix (within raw_datalake_bucket) for a source version's raw files."""
    return f"{_raw_landing_root}/{source}/{version}"


def raw_storage_uri() -> str:
    """Full s3a:// root the Spark job reads raw data from (overrides the baked config at runtime)."""
    return f"s3a://{raw_datalake_bucket}/{_raw_landing_root}"


# DAGs settings

DAG_ID_PREFIX = "opendatalake"
DAG_DEFAULT_TAGS = [DAG_ID_PREFIX]
DAG_DISPLAY_NAME_PREFIX = "Open Datalake"

# Assets settings
ASSETS_URI_PREFIX = "opendatalake"


# Pools
DOWNLOAD_TASKS_POOL = "opendatalake_download_tasks_pool"
