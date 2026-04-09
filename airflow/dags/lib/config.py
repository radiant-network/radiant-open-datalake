# Importing from airflow.models instead of airflow.sdk to avoid test initialization issues.
# This is fixed in newer Airflow versions, so you can switch back to airflow.sdk if we upgrade.
from airflow.models import Variable

# S3 settings
environment = Variable.get("environment", "dev")
s3_conn_id = "opendatalake_s3"
raw_datalake_bucket = f"opendatalake-{environment}"

# DAGs settings
DAG_DEFAULT_TAGS = ["opendatalake"]
DAG_ID_PREFIX = "opendatalake"
DAG_DISPLAY_NAME_PREFIX = "Open Datalake"

# Assets settings
ASSETS_URI_PREFIX = "opendatalake"
ASSETS_NAME_PREFIX = "opendatalake"
