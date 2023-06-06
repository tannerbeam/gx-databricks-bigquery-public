from utils import notebook_utils
import pandas_gbq
from google.oauth2 import service_account
from typing import Optional, Union
from datetime import datetime
from dataclasses import dataclass, field

import great_expectations as gx
from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext as Context,
)

import inspect
import os
import sys

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

@dataclass
class RepoConfig:
    """
    Dataclass for repo config parameters. Read hardcoded values
    from `config.yml` then pass to cls using `to_dataclass()` method.
    """

    ts: str = datetime.now()
    nb_name: str = notebook_utils.Notebook().name
    repo_directory: Optional[str] = "dev"
    repo_name: Optional[str] = None
    bigquery_creds_file: Optional[str] = None
    gx_connector_name: Optional[str] = "pandas_runtime"
    params: dict[str] = field(
        default_factory=lambda: {
            "param_dt_begin": notebook_utils.default_query_date(),
            "param_dt_end": notebook_utils.default_query_date(),
            "param_pypi_pkg": "great-expectations",
        }
    )
    gx_version: str = gx.__version__
    gx_dir: Optional[str] = None
    batch_ids: dict[str, str] = field(
        default_factory=lambda: {
            "batch_ids": {"dt": notebook_utils.default_query_date()}
        }
    )
    gx_cloud_base_url: str = "https://api.greatexpectations.io"
    gx_cloud_organization_id: str = "3cd57c8a-611b-4393-a800-b633f0137c74"
    gx_cloud_access_token: str = dbutils.secrets.get(
        scope="analytics", key="cloud-token"
    )

    def __post_init__(self):
        self.asset_name: str = self.nb_name
        self.datasource_name: str = f"{self.nb_name}_{self.gx_connector_name}"
        self.expectation_suite_name: str = f"{self.nb_name}_{self.gx_connector_name}"
        self.tld: str = f"/Workspace/Repos/{self.repo_directory}/{self.repo_name}"
        self.gx_tld: str = self.create_gx_dir()
        self.gbq_context: pandas_gbq.gbq.Context = self.pandas_gbq_context()
        self.cloud_env_vars: list[str] = self.set_cloud_env_vars()
        self.attributes: dict[str] = {k: v for k, v in self.__dict__.items()}

    @classmethod
    def to_dataclass(cls, config: dict):
        """
        Parse a config dict and convert to dataclass instance
        """
        return cls(
            **{
                key: (
                    config[key]
                    if val.default == val.empty
                    else config.get(key, val.default)
                )
                for key, val in inspect.signature(RepoConfig).parameters.items()
            }
        )

    def set_cloud_env_vars(self) -> list[str]:
        """
        Use os.environ to set gx cloud credentials
        """

        os_vars: dict[str] = {
            "GE_CLOUD_BASE_URL": self.gx_cloud_base_url,
            "GE_CLOUD_ORGANIZATION_ID": self.gx_cloud_organization_id,
            "GE_CLOUD_ACCESS_TOKEN": self.gx_cloud_access_token,
        }

        for k, v in os_vars.items():
            os.environ[k] = v

        return [var for var in os.environ if var in os_vars.keys()]

    def create_gx_dir(self) -> str:
        """
        Create GX top level dir if not exists
        """
        if not self.gx_dir in os.listdir(self.tld):
            os.makedirs(f"{self.tld}/{self.gx_dir}")

        return f"{self.tld}/{self.gx_dir}"

    def set_nb_params(self) -> None:
        """
        Set databricks notebook parameters
        """
        notebook_utils.get_nb_params_from_dict(self.params)

    def pandas_gbq_context(self) -> pandas_gbq.gbq.Context:
        """
        Use API credentials to get a pandas_gbq context.
        Args:
            - filename: json file in repo top level directory w/ service account credentials
        Returns: gbq.context object
        """
        filename = self.bigquery_creds_file

        if not filename in os.listdir(self.tld):
            raise FileNotFoundError(
                f"Unable to find file named '{filename}' in {self.tld}."
            )

        filepath = f"{self.tld}/{filename}"

        creds = service_account.Credentials.from_service_account_file(filepath)
        pandas_gbq.context.credentials = creds
        pandas_gbq.context.project = creds.project_id
        return pandas_gbq.context


def get_repo_config(config_file: str) -> RepoConfig:
    """
    Get a RepoConfig object from a config.yml file
    """
    config = notebook_utils.read_yaml(config_file)
    rc = RepoConfig.to_dataclass(config)
    rc.set_nb_params()

    return rc