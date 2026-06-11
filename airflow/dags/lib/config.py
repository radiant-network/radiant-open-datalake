"""
Holds configuration values and global constants for the project.

Note: Some Airflow or environment variables may be missing. This is intentional,
as certain operators only require a subset of the configuration and do not need
all variables to be present.
"""

# Importing from airflow.models instead of airflow.sdk to avoid test initialization issues.
# This is fixed in newer Airflow versions, so you can switch back to airflow.sdk if we upgrade.
from airflow.models import Variable

_MISSING_VARIABLE = "MISSING"

# S3 settings
environment = Variable.get("environment", "dev")
s3_conn_id = "opendatalake_s3"
raw_datalake_bucket = f"opendatalake-{environment}"

# DAGs settings

DAG_ID_PREFIX = "opendatalake"
DAG_DEFAULT_TAGS = [DAG_ID_PREFIX]
DAG_DISPLAY_NAME_PREFIX = "Open Datalake"

# Assets settings
ASSETS_URI_PREFIX = "opendatalake"


# Pools
DOWNLOAD_TASKS_POOL = "opendatalake_download_tasks_pool"
