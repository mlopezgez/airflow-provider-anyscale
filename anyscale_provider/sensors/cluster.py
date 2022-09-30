from typing import Sequence
from anyscale import AnyscaleSDK

from airflow.utils.context import Context
from airflow.sensors.base import BaseSensorOperator
from airflow.compat.functools import cached_property


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


class AnyscaleClusterSensor(AnyscaleBaseClusterSensor):

    template_fields: Sequence[str] = [
        "auth_token",
        "cluster_id",
    ]

    def __init__(
        self,
        *,
        cluster_id: str,
        **kwargs,
    ):
    
        super().__init__(**kwargs)
        self.cluster_id = cluster_id

    def _log_services(self, response):
        services = response.result.services_urls

        if services:
            self.log.info("service urls:")
            for name, service in services.to_dict().items():
                self.log.info("%s: %s", name, service)

    def _log_head_node(self, response):
        head_node_info = response.result.head_node_info

        if head_node_info:
            self.log.info("head node info:")
            for name, info in head_node_info.to_dict().items():
                self.log.info("%s: %s", name, info)

    def poke(self, context: Context) -> bool:

        response = self.sdk.get_cluster(self.cluster_id)

        state = response.result.state
        goal_state = response.result.goal_state

        self.log.info("current state: %s, goal state: %s", state, goal_state)

        if goal_state is not None:
            return False

        self.log.info("cluster reached goal state: %s", state)
        self._log_head_node(response)
        self._log_services(response)

        return True
