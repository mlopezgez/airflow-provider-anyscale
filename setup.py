import os
from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

VERSION = os.environ.get("ANYSCALE_PROVIDER_VERSION", "0.0.1")

setup(
    name="airflow-provider-anyscale",
    description="An Apache Airflow provider for Anyscale",
    entry_points={
        "apache_airflow_provider": [
            "provider_info=anyscale_provider.__init__:get_provider_info"
        ]
    },
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="Apache License 2.0",
    version=VERSION,
    packages=["anyscale_provider",
              "anyscale_provider.sensors", "anyscale_provider.operators"],
    install_requires=["apache-airflow>=2.0", "anyscale==0.5.50"],
    setup_requires=["setuptools", "wheel"],
    extras_require={},
    author="Matias Lopez",
    author_email="matias.lopez@anastasia.ai",
    maintainer="Matías López",
    maintainer_email="matias.lopez@anastasia.ai",
    keywords=["anyscale", "ray", "distributed", "compute", "airflow"],
    python_requires="~=3.7",
    include_package_data=True,
)
