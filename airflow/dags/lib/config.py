from airflow.sdk import Variable

# S3 settings
environment = Variable.get("environment", "dev")
s3_conn_id = "opendatalake_s3"
raw_datalake_bucket = f"opendatalake-{environment}"

# DAGs settings
dag_default_tags = ["opendatalake"]
dag_id_prefix = "opendatalake"
dag_display_name_prefix = "Open Datalake"

# Assets settings
assets_uri_prefix = "opendatalake"
assets_name_prefix = "opendatalake"
