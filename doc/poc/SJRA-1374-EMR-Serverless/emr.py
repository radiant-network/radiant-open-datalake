import logging
from typing import Any

from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.utils.context import Context


class EmrServerlessStartJobWithLogsOperator(EmrServerlessStartJobOperator):
    """EMR Serverless start-job operator that pipes Spark driver logs into the Airflow task log.

    Injects a CloudWatch monitoring config into the EMR job request (unless caller already set
    one), runs the job to completion, then forwards SPARK_DRIVER stdout/stderr from CloudWatch
    Logs into the task log — even on job failure.
    """

    template_fields = (
        *EmrServerlessStartJobOperator.template_fields,
        "cloudwatch_log_group",
        "cloudwatch_log_stream_prefix",
    )

    def __init__(
        self,
        *,
        cloudwatch_log_group: str,
        cloudwatch_log_stream_prefix: str | None = None,
        cloudwatch_region: str | None = None,
        **kwargs,
    ):
        kwargs.setdefault("wait_for_completion", True)
        kwargs["configuration_overrides"] = self._merge_monitoring(
            kwargs.get("configuration_overrides"),
            cloudwatch_log_group,
            cloudwatch_log_stream_prefix,
        )
        super().__init__(**kwargs)
        self.cloudwatch_log_group = cloudwatch_log_group
        self.cloudwatch_log_stream_prefix = cloudwatch_log_stream_prefix
        self.cloudwatch_region = cloudwatch_region

    @staticmethod
    def _merge_monitoring(overrides: dict | None, log_group: str, stream_prefix: str | None) -> dict:
        overrides = dict(overrides or {})
        monitoring = dict(overrides.get("monitoringConfiguration") or {})
        cw = dict(monitoring.get("cloudWatchLoggingConfiguration") or {})
        cw.setdefault("enabled", True)
        cw.setdefault("logGroupName", log_group)
        if stream_prefix and "logStreamNamePrefix" not in cw:
            cw["logStreamNamePrefix"] = stream_prefix
        monitoring["cloudWatchLoggingConfiguration"] = cw
        overrides["monitoringConfiguration"] = monitoring
        return overrides

    def execute(self, context: Context) -> Any:
        try:
            return super().execute(context)
        finally:
            self._forward_driver_logs()

    def _forward_driver_logs(self) -> None:
        log = logging.getLogger("airflow.task")
        job_run_id = getattr(self, "job_id", None)
        if not job_run_id:
            log.warning("No job_run_id available; skipping driver log forwarding.")
            return

        hook = AwsLogsHook(aws_conn_id=self.aws_conn_id, region_name=self.cloudwatch_region)
        base = f"applications/{self.application_id}/jobs/{job_run_id}/SPARK_DRIVER"
        if self.cloudwatch_log_stream_prefix:
            base = f"{self.cloudwatch_log_stream_prefix}/{base}"
        not_found = hook.get_conn().exceptions.ResourceNotFoundException

        for kind in ["stdout", "stderr"]:
            stream = f"{base}/{kind}"
            log.info("===== SPARK_DRIVER/%s (%s) =====", kind, stream)
            try:
                empty = True
                for evt in hook.get_log_events(log_group=self.cloudwatch_log_group, log_stream_name=stream):
                    log.info(evt["message"])
                    empty = False
                if empty:
                    log.info("(no events)")
            except not_found:
                log.warning("Stream not found: %s", stream)
