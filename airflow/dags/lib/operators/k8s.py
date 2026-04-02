
from airflow.sdk import task


@task()
def transfer_file_via_local_copy(source: str, version: str, download_idx: int) -> dict:
    #TODO implement this function using a virtualenv operator that will execute the local copy logic in a separate environment with the necessary dependencies installed
    pass

#TODO
#def _get_k8s_context() -> dict:
#    return {}




#TODO: check this
#@task.kubernetes(
#    **dict(
#        pool="opendatalake_download".
#        do_xcom_push=True,
#    )
#    | _get_k8s_context()
#)
#def transfer_file_via_local_copy(source: str, version: str, download_idx: int) -> dict:
#    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
#    from dags.lib.config import s3_conn_id, raw_datalake_bucket
#    from dags.lib.domain.download import upload_via_local_copy_to_s3
#
#    from dags.lib.domain.model.sources import get_download_configs
#
#    upload_via_local_copy_to_s3(
#            s3=S3Hook(s3_conn_id),
#            s3_bucket=raw_datalake_bucket,
#            s3_prefix=f"raw/{source}/{version}",
#            version=version,
#            download_conf=get_download_configs(source)[download_idx]
#        )

#    return transfer_file_via_local_copy(source=source, version=version, download_idx=download_idx)