from typing import Sequence

from airflow.utils.context import Context
from plugins.utils import push_to_xcom

from plugins.providers.anyscale.hooks.anyscale_hook import AnyscaleHook

from airflow.exceptions import AirflowException

from airflow.sensors.base_sensor_operator import BaseSensorOperator


class AnyscaleServiceSensor(BaseSensorOperator):

    template_fields: Sequence[str] = [
        "service_id",
        "auth_token",
    ]

    def __init__(
        self,
        service_id: str,
        auth_token: str,
        goal_state: str = "RUNNING",
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.service_id = service_id
        self.auth_token = auth_token
        self.goal_state = goal_state
        self._ignore_keys = []

    def poke(self, context: Context) -> bool:

        hook = AnyscaleHook(auth_token=self.auth_token)
        sdk = hook.get_sdk()

        response = sdk.get_service(
            service_id=self.service_id)

        state = response.result.state

        msg = (
            f"current state: {state.current_state}, "
            f"goal state: {self.goal_state}"
        )

        self.log.info(msg)

        operation_message = state.operation_message

        if operation_message:
            self.log.info(operation_message)

        if state.current_state in ("OUT_OF_RETRIES", "TERMINATED", "ERRORED", "BROKEN"):
            if self.goal_state == state.current_state:
                return True

            msg = (
                f"job ended with status {state.current_state}, "
                f"error: {state.error}"
            )
            raise AirflowException(msg)

        if state.current_state != self.goal_state:
            return False

        self.log.info(
            f"service {self.service_id} reached goal state {self.goal_state}")

        took = response.result.state.state_transitioned_at - response.result.created_at
        self.log.info(f"duration: {took.total_seconds()}")
        self.log.info(f"service available at: {response.result.url}")

        return True
