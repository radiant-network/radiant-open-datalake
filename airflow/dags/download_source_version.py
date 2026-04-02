from airflow.sdk import Metadata, dag, task

from dags.lib import config
from dags.lib.assets import new_source_version_asset, raw_dataset_asset


if config.environment == "dev":
    from dags.lib.operators.k8s import transfer_file_via_local_copy
else:
    from dags.lib.operators.ecs import transfer_file_via_local_copy


#TODO add parameters for manual trigger?
#TODO add force parameter
#Might want to force the detection of a new version
@dag(
    dag_display_name=f"{config.dag_display_name_prefix} - Download Source Version",
    dag_id=f"{config.dag_id_prefix}-download-source-version",
    schedule=new_source_version_asset,
    tags=config.dag_default_tags
)
def download_source_version():

    # TODO one task to parse the version either from the asset or
    # from dag parameters
    # TODO multiple output ?
    @task
    def get_source_and_version(triggering_asset_events=None):
        metadata = triggering_asset_events[new_source_version_asset][-1].extra
        return {
            "source": metadata["source"], 
            "version": metadata["latest_version"]
        }


    #TODO check how to emit .. need to access airflow context
    @task.virtualenv(requirements=["requests==2.32.5"])
    def direct_transfer(source, version, download_idx):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from dags.lib.config import s3_conn_id, raw_datalake_bucket
        from dags.lib.domain.download import direct_upload_to_s3
        from dags.lib.domain.model.sources import get_download_configs

        direct_upload_to_s3(
            s3=S3Hook(s3_conn_id),
            s3_bucket=raw_datalake_bucket,
            s3_prefix=f"raw/{source}/{version}",
            version=version,
            download_conf=get_download_configs(source)[download_idx]
        )

    

        #TODO more simple to just use an ECS operator with custom image, so that it
        #always works??
        #Better to have a more granular approach ... i.e. will create specific tasks
        #for each download config and group them in a task group for the source?
        #might be more complex to maintain but will have better visibility and retry capabilities in case of failure of one of the download configs

        #TODO... we might need to guarantee that the download configs are always returned in the
        #same order ...
        #Maybe just add some identifier and use it here???


    @task(outlets=[raw_dataset_asset])
    def emit_raw_dataset_event(source, version):
        print("TODO")
    
    get_source_and_version_task = get_source_and_version()


download_source_version()