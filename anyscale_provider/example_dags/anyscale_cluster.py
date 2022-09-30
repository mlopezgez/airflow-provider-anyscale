from datetime import datetime

from airflow.decorators import dag
from airflow.utils.trigger_rule import TriggerRule

from anyscale_provider.operators.cluster import (
    AnyscaleCreateClusterOperator,
    AnyscaleStartClusterOperator,
    AnyscaleTerminateClusterOperator
)
from anyscale_provider.operators.session_command import (
    AnyscaleCreateSessionCommandOperator
)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2022, 9, 30),
    tags=["demo"],
)
def anyscale_cluster():

    cluster = AnyscaleCreateClusterOperator(
        task_id="create_cluster",
        name="<name>",
        project_id="<project_id>",
        compute_config_id="<compute_config_id>",
        cluster_environment_build_id="<cluster_environment_build_id>",
        auth_token="<auth_token>",
    )

    start = AnyscaleStartClusterOperator(
        task_id="start_cluster",
        cluster_id=cluster.output["id"],
        auth_token="<auth_token>",
        wait_for_completion=True,
    )

    job = AnyscaleCreateSessionCommandOperator(
        task_id="submit_job",
        auth_token="<auth_token>",
        session_id=cluster.output["id"],
        shell_command="python3 -c 'import ray'",
        wait_for_completion=True,

    )

    terminate = AnyscaleTerminateClusterOperator(
        task_id="terminate_cluster",
        auth_token="<auth_token>",
        cluster_id=cluster.output["id"],
        wait_for_completion=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    cluster >> start >> job >> terminate


dag = anyscale_cluster()
