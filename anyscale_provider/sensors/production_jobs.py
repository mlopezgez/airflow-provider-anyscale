from typing import Sequence
from anyscale import AnyscaleSDK

from airflow.utils.context import Context
from airflow.compat.functools import cached_property

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator


class AnyscaleBaseClusterSensor(BaseSensorOperator):
    def __init__(
        self,
        *,
        auth_token: str,
        **kwargs
    ):

        self.auth_token = auth_token
        super.__init__(**kwargs)

    @cached_property
    def sdk(self) -> AnyscaleSDK:
        return AnyscaleSDK(auth_token=self.auth_token)

    def poke(self, context: Context) -> bool:
        raise NotImplementedError("Please implement poke() in subclass")


class AnyscaleProductionJobSensor(BaseSensorOperator):

    template_fields: Sequence[str] = [
        "production_job_id",
        "auth_token",
    ]

    def __init__(
        self,
        *,
        production_job_id: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.production_job_id = production_job_id

    def _fetch_logs(self):
        response = self.sdk.get_production_job_logs(self.production_job_id)
        return response.result.logs

    def poke(self, context: Context) -> bool:

        response = self.sdk.get_production_job(
            production_job_id=self.production_job_id)

        state = response.result.state

        self.log.info("current state: %s, goal state %s", state.current_state, state.goal_state)

        operation_message = state.operation_message
        if operation_message:
            self.log.info(operation_message)

        if state.current_state in ("OUT_OF_RETRIES", "TERMINATED", "ERRORED"):
            raise AirflowException(
                "job ended with status {}, error: {}".format(
                    state.current_state,
                    state.error,
                )
            )

        if state.current_state != state.goal_state:
            return False

        self.log.info(
            "job %s reached goal state %s", state.production_job_id, state.goal_state)

        took = response.result.state.state_transitioned_at - response.result.created_at

        self.log.info("duration: %s", took.total_seconds())

        try:
            logs = self._fetch_logs()
            self.log.info("logs: \n %s", logs)

        except Exception:
            self.log.warning("logs not found for %s", self.production_job_id)
        
        return True
