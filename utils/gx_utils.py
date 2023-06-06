from utils.repo_utils import get_repo_config
from utils.iso3166 import country_codes

import great_expectations as gx
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext as Context,
)
from great_expectations.datasource.fluent.pandas_datasource import DataFrameAsset
from great_expectations.checkpoint import Checkpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.datasource.fluent.batch_request import BatchRequest
from great_expectations.validator.validator import Validator

import json
import os
import re
from typing import Optional, Union
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

import pandas as pd
from pandas.core.frame import DataFrame as PandasDataFrame


def find_config_file() -> str:
    paths = []
    depths = [".", "../", "../../", "../../.."]
    for depth in depths:
        for root, dirs, files in os.walk(depth):
            for file in files:
                if file.lower() == "config.yml":
                    paths.append(os.path.join(root, file))
    res = paths
    res_list = [r for r in res if r == "config.yml" or r.endswith("./config.yml")]
    return res[0]


# path to repo config file
rc = get_repo_config(find_config_file())


def default_context() -> Context:
    """
    Get an in-memory (ephemeral) data context based on repo config.
    Use fluent datasources (GX v16.0 and above).
    Args:
        - pandas_df: pandas dataframe to be validated
    Returns:
        GX CloudContext w/ config
    """
    context = gx.get_context(ge_cloud_mode=True)

    datasource_name = rc.datasource_name
    asset_name = rc.asset_name

    datasources = list(context.datasources.keys())

    # add the default datasource if it doesn't already exist
    if not datasource_name in datasources:
        print(f"Adding default datasource '{datasource_name}' to context...")
        context.sources.add_or_update_pandas(datasource_name)

    return context


def default_asset(
    pandas_df: PandasDataFrame,
    context: Optional[Context] = None,
) -> DataFrameAsset:
    """
    Create a default batch request from pandas_df.
    """

    if not context:
        context = default_context()

    datasource_name = rc.datasource_name
    datasource = context.get_datasource(datasource_name)
    asset_name = rc.asset_name
    expectation_suite_name = rc.expectation_suite_name

    try:
        data_asset = datasource.get_asset(asset_name)
    except LookupError:
        datasource.add_dataframe_asset(name=asset_name, dataframe=pandas_df)
        data_asset = datasource.get_asset(asset_name)

    return data_asset


def default_validations(
    pandas_df: PandasDataFrame, date_range: list[str], validator: Validator
) -> Validator:
    """
    Add validation rules (aka Expectations) to a Validator
    """

    # all columns
    all_cols = [col for col in pandas_df.columns]

    # columns expected to not have any null values
    notnull_cols = ["download_time", "country_code", "file_type", "pkg_version", "dt"]

    # columns expected to have some null values
    somenull_cols = [col for col in all_cols if not col in notnull_cols]

    # validate: expect not null
    [validator.expect_column_values_to_not_be_null(column=c) for c in notnull_cols]

    # validate: expect ~mostly~ not nulls (95%)
    [
        validator.expect_column_values_to_not_be_null(column=c, mostly=0.95)
        for c in somenull_cols
    ]

    # columns expected to be of timestamp type
    time_cols = ["download_time"]

    # validate: expect timestamp type
    [
        validator.expect_column_values_to_be_of_type(column=c, type_="Timestamp")
        for c in time_cols
    ]

    # column values of timestamps in `download_time` expected to be between
    ts_range = [
        pd.Timestamp(date_range[0], tz="UTC"),
        pd.Timestamp(date_range[1], tz="UTC")
        + timedelta(hours=23, minutes=59, seconds=59),
    ]

    # set evaluation params (will change based on nb param values!)
    validator.set_evaluation_parameter("min_ts", min(ts_range))
    validator.set_evaluation_parameter("max_ts", max(ts_range))

    # validate: expect timestamps in range
    validator.expect_column_values_to_be_between(
        column="download_time",
        min_value={"$PARAMETER": "min_ts"},
        max_value={"$PARAMETER": "max_ts"},
    )

    # column values in `country_code` expected to mostly (99%) match set
    iso_country_codes = list(country_codes)

    # validate: expect country codes to be in set
    validator.expect_column_values_to_be_in_set(
        column="country_code", value_set=iso_country_codes, mostly=0.99
    )

    # map of most common values by column
    top_values_map = {
        "file_type": "bdist_wheel",
        "installer_name": "pip",
        "distro_name": "Ubuntu",
        "system_name": "Linux",
    }

    # validate: expect most common values
    [
        validator.expect_column_most_common_value_to_be_in_set(column=k, value_set=[v])
        for k, v in top_values_map.items()
    ]

    # column values of `pkg_version` expected to match regex
    pkg_version_regex = r"^(\d{1}\.\d{1,2}\.\d{1,2}$)"

    # validate: expect mostly (0.95) regex match
    validator.expect_column_values_to_match_regex(
        column="pkg_version", regex=pkg_version_regex, mostly=0.95
    )

    return validator


def default_validator(
    pandas_df: PandasDataFrame,
    date_range: list[str],
    context: Optional[Context] = None,
    batch_request: Optional[BatchRequest] = None,
    overwrite: Optional[bool] = False,
) -> Validator:
    """
    Create a Validator from existing expectation suite in GX directory or from the rules defined in default_validations().
    Args:
        - pandas_df: pandas dataframe to be validated
        - batch_request: a FluentBatchRequest for an in-memory DataFrame
        - date_range: list of ISO-8601 dates (e.g.['1970-01-01', '1970-12-31']) with starting/ending dates for the dataframe
        - context: a GX CloudContext
        - overwrite: True to overwrite the existing expectation suite in GX directory with rules defined in default_validations().
    Returns:
        - a GX Validator that can be passed to a GX Checkpoint for dataframe validation.
    """
    if not context:
        context = default_context()

    if not batch_request:
        batch_request = default_batch_request(pandas_df)

    expectation_suite_names = context.list_expectation_suite_names()
    expectation_suite_name = rc.expectation_suite_name

    # create validator with default validations added to the expectation suite
    if overwrite:
        print(
            f"Creating new Validator by overwriting existing expectation suite '{expectation_suite_name}' in GX directory with validation rules defined in default_validations()."
        )

        if expectation_suite_name in expectation_suite_names:
            expectation_suite = context.get_expectation_suite(expectation_suite_name)
        else:
            context.add_or_update_expectation_suite(
                expectation_suite_name=expectation_suite_name, expectations=None
            )

        validator = context.get_validator(
            expectation_suite_name=expectation_suite_name,
            batch_request=batch_request,
        )

        validator = default_validations(pandas_df, date_range, validator)

        # will persist the expectation suite to disk as json
        validator.save_expectation_suite()

    # otherwise create validator with expectations associated with the expectation_suite_name
    else:
        print(
            f"Creating Validator using existing expectation suite '{expectation_suite_name}'."
        )
        validator = context.get_validator(
            expectation_suite_name=expectation_suite_name,
            batch_request=batch_request,
        )

    return validator


def default_action_list(slack_webhook: Optional[str] = None) -> list[dict[str]]:

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

    if not slack_webhook:
        slack_webhook = dbutils.secrets.get(
            scope="analytics_pipeline",
            key="analytics_pipeline_status_slack_bot_webhook",
        )

    return
    (
        [
            {
                "name": "send_slack_notification_on_validation_result",  # name can be set to any value
                "action": {
                    "class_name": "SlackNotificationAction",
                    "slack_webhook": slack_webhook,
                    "notify_on": "all",  # possible values: "all", "failure", "success"
                    "renderer": {
                        "module_name": "great_expectations.render.renderer.slack_renderer",
                        "class_name": "SlackRenderer",
                    },
                },
            },
            {
                "name": "store_validation_result",
                "action": {
                    "class_name": "StoreValidationResultAction",
                },
            },
            {
                "name": "store_evaluation_params",
                "action": {
                    "class_name": "StoreEvaluationParametersAction",
                },
            },
        ]
    )


def default_checkpoint_config(
    pandas_df: PandasDataFrame,
    checkpoint_name: Optional[str] = None,
    action_list: Optional[list[dict[str]]] = None,
) -> dict:

    expectation_suite_name = rc.expectation_suite_name

    if not checkpoint_name:
        checkpoint_name = expectation_suite_name

    if not action_list:
        action_list = default_action_list()

    return {
        "config_version": 1,
        "class_name": "Checkpoint",
        "name": checkpoint_name,
        "batch_request": default_batch_request(pandas_df),
        "expectation_suite_name": expectation_suite_name,
        "action_list": action_list,
    }


def default_checkpoint(
    pandas_df: PandasDataFrame,
    context: Optional[Context] = None,
    action_list: Optional[list[dict[str]]] = None,
) -> Checkpoint:
    """
    Create a default checkpoint from config.
    """
    if not context:
        context = default_context()

    checkpoint_name = rc.expectation_suite_name
    checkpoints = [c.resource_name for c in context.list_checkpoints()]

    if not checkpoint_name in checkpoints:
        checkpoint_config = default_checkpoint_config(pandas_df)
        context.add_or_update_checkpoint(**checkpoint_config)
        checkpoint = context.get_checkpoint(checkpoint_name)
    else:
        checkpoint = context.get_checkpoint(checkpoint_name)

    return checkpoint


class CheckpointFailedException(Exception):
    """
    Exception to fail workflow based on checkpoint results
    """

    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = None

    def __str__(self):
        if self.message:
            return "CheckpointFailed: {0} ".format(self.message)
        else:
            return "CheckpointFailedException"


class CheckpointRunResult(CheckpointResult):
    """
    Extend CheckpointResult class
    """

    def __init__(
        self,
        result: CheckpointResult,
        run_stats: Optional[dict[Union[str, int]]] = None,
    ) -> None:
        super().__init__(
            run_id=result.run_id,
            run_results=result.run_results,
            checkpoint_config=result.checkpoint_config,
        )

        self.batch_ids = rc.batch_ids

        def get_run_stats(self) -> dict[Union[str, int]]:
            """
            Get dict of summary info about expectation evaluations
            """
            stats = self.get_statistics()
            stats_dict = list(stats["validation_statistics"].items())[0][1]
            vld_total = stats_dict.get("evaluated_expectations")
            vld_failed = stats_dict.get("unsuccessful_expectations")

            return {
                "evaluated": vld_total,
                "failed": vld_failed,
                "passed": vld_total - vld_failed,
            }

        self.run_stats = get_run_stats(self)

        def get_validation_results(self) -> dict:
            """
            Get validation results
            """
            return self.list_validation_results()[0]

        self.validation_results = get_validation_results(self)

        def get_validation_id(self) -> Union[str, None]:
            """
            Get validation id from run results
            """
            results = self.validation_results
            return results.get("meta").get("validation_id")

        self.validation_id = get_validation_id(self)

        def get_dates(self) -> dict[str]:
            """
            Get run date and data date
            """
            return {
                "run_dt": self.run_id.run_time.strftime("%Y-%m-%d"),
                "data_dt": next(iter(rc.batch_ids.values())),
            }

        self.dates = get_dates(self)

        def get_failed_expectations(self) -> Union[list[dict[str]], None]:
            """
            Get list of failed expectations.
            """
            res_failed = []
            for vr in result.list_validation_results():
                for res in vr.get("results"):
                    if not res.get("success"):
                        res_failed.append(res)

            if len(res_failed) == 0:
                return None
            else:
                return res_failed

        self.failed_expectations = get_failed_expectations(self)

    def check_results(self, failures_allowed: int = 0) -> None:
        """
        Raise exception if failures > failures_allowed
        """
        num_failures = self.run_stats["failed"]
        if num_failures > failures_allowed:
            raise CheckpointFailedException(
                f"Number of failures ({num_failures}) exceeds failures allowed ({failures_allowed})."
            )
        else:
            print(
                f"Number of failures ({num_failures}) at or below failures allowed ({failures_allowed})."
            )

    def list_failures(self) -> None:
        """
        Print info about failed expectations.
        """
        failures = self.failed_expectations
        if not failures:
            print("No failed expectations to list.")
        else:
            for f in failures:
                print(f)