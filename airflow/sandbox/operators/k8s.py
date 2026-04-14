from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

import os

from dags.lib import config


def _get_k8s_context():
    # Note: passing the s3 connection variable as env vars for now
    # There might be a better way to pass the connection info to the container 
    s3_conn_variable_prefix = "AIRFLOW_CONN_" + config.s3_conn_id.upper()
    s3_conn_variables = {k: v for k,v in os.environ.items() if k.startswith(s3_conn_variable_prefix)}

    return dict(
        namespace="opendatalake",
        image="ghcr.io/radiant-network/opendatalake-airflow-task-operator:latest",
        image_pull_policy="IfNotPresent",
        get_logs=True,
        is_delete_operator_pod=True,
        env_vars=s3_conn_variables
    )


class PythonScriptOperator(KubernetesPodOperator):
    def __init__(self, script_name, script_args, **kwargs):
        assert "cmds" not in kwargs, "Don't pass cmds: generated dynamically."

        super().__init__(**_get_k8s_context(), **kwargs)
        self.template_fields = self.template_fields + ("script_args", "script_name")

        self.script_name = script_name
        self.script_args = script_args


    def execute(self, context, **kwargs):
        self.cmds = ["python", self.script_name]
        for k, v in self.script_args.items():
            self.cmds.append(f"--{k}")
            self.cmds.append(str(v))
        return super().execute(context, **kwargs)

