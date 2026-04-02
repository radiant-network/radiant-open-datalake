
from airflow.sdk import task


@task()
def transfer_via_local_copy(source: str, version: str, download_idx: int) -> dict:
    #TODO implement this function using a virtualenv operator that will execute the local copy logic in a separate environment with the necessary dependencies installed
    pass