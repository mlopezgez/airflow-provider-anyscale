import json
from typing import Sequence

from airflow.utils.context import Context

from airflow.exceptions import AirflowException
from anyscale_provider.sensors.base import AnyscaleBaseSensor


class AnyscaleSessionCommandSensor(AnyscaleBaseSensor):

    template_fields: Sequence[str] = [
        "session_command_id",
        "auth_token",
    ]

    def __init__(
        self,
        session_command_id: str,
        auth_token: str,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.auth_token = auth_token
        self.session_command_id = session_command_id

    def poke(self, context: Context) -> bool:

        session_command_response = self.sdk.get_session_command(
            self.session_command_id).result

        status_code = session_command_response.status_code

        if status_code is None:
            return False

        took = session_command_response.finished_at - session_command_response.created_at

        self.log.info("duration: %s", took.total_seconds())
        self.log.info(
            "session command %s ended with status code %s",
            self.session_command_id,
            session_command_response.status_code
        )

        if status_code != 0:
            raise AirflowException("session command ended with errors")

        return True
