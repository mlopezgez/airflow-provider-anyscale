import time
from typing import Optional, Sequence

from airflow.utils.context import Context
from airflow.exceptions import AirflowException
from airflow.utils.log.secrets_masker import mask_secret

from anyscale_provider.utils import push_to_xcom
from anyscale_provider.operators.base import AnyscaleBaseOperator
from anyscale_provider.sensors.services import AnyscaleServiceSensor

from anyscale.shared_anyscale_utils.utils.byod import BYODInfo
from anyscale.sdk.anyscale_client.models.create_production_service import CreateProductionService


class AnyscaleApplyServiceOperator(AnyscaleBaseOperator):

    template_fields: Sequence[str] = [
        "name",
        "auth_token",
        "project_id",
        "entrypoint",
        "cluster_environment_build_id",
        "docker",
        "description",
        "runtime_env",
        "compute_config_id",
        "ray_version",
        "python_version",
        "access",
    ]

    def __init__(
        self,
        name: str,
        project_id: str,
        entrypoint: str,
        healthcheck_url: str,
        cluster_environment_build_id: str = None,
        docker: str = None,
        max_retries: int = 0,
        access: str = "private",
        description: str = None,
        runtime_env: dict = None,
        compute_config_id: str = None,
        ray_version: Optional[str] = None,
        python_version: Optional[str] = None,
        wait_for_completion: Optional[bool] = False,
        poke_interval: Optional[int] = 60,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.name = name
        self.docker = docker
        self.access = access
        self.entrypoint = entrypoint
        self.project_id = project_id
        self.description = description
        self.max_retries = max_retries
        self.runtime_env = runtime_env
        self.healthcheck_url = healthcheck_url
        self.compute_config_id = compute_config_id
        self.ray_version = ray_version or "1.13.0"
        self.python_version = python_version or "py38"
        self.cluster_environment_build_id = cluster_environment_build_id

        self.wait_for_completion = wait_for_completion
        self.poke_interval = poke_interval
        self._ignore_keys = []

    def _get_cluster_environment_build_id(self) -> str:

        cluster_environment_build_id = None

        if self.docker:

            cluster_environment_build_id = BYODInfo(
                docker_image_name=self.docker,
                python_version=self.python_version,
                ray_version=self.ray_version,
            ).encode()

        if self.cluster_environment_build_id:
            if self.docker:
                self.log.info(
                    "docker is ignored when cluster_environment_build_id is provided.")

            cluster_environment_build_id = self.cluster_environment_build_id

        if cluster_environment_build_id is None:
            raise AirflowException(
                "at least cluster_environment_build_id or docker must be provided.")

        return cluster_environment_build_id

    def execute(self, context: Context) -> None:

        cluster_environment_build_id = self._get_cluster_environment_build_id()

        create_production_service = CreateProductionService(
            name=self.name,
            access=self.access,
            description=self.description,
            project_id=self.project_id,
            healthcheck_url=self.healthcheck_url,
            config={
                "entrypoint": self.entrypoint,
                "build_id": cluster_environment_build_id,
                "runtime_env": self.runtime_env,
                "compute_config_id": self.compute_config_id,
                "max_retries": self.max_retries,
            },
        )

        production_service = self.sdk.apply_service(
            create_production_service).result

        self.log.info("production service %s created", production_service.id)

        if self.wait_for_completion:
            while not AnyscaleServiceSensor(
                task_id="wait_service",
                service_id=production_service.id,
                auth_token=self.auth_token,
            ).poke(context):

                time.sleep(self.poke_interval)

            self.log.info("service available at %s", production_service.url)

        xcom_payload = production_service.to_dict()
        xcom_payload["token"] = mask_secret(xcom_payload["token"])
        push_to_xcom(xcom_payload, context, self._ignore_keys)
