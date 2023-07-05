# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import great_expectations as gx
import pandas as pd
import datetime

# COMMAND ----------

df = pd.read_pickle("../pypi_sample_data_great-expectations.pkl")

# COMMAND ----------

context = gx.get_context(cloud_mode=False)

# COMMAND ----------

datasource_name = expectation_suite_name = checkpoint_name = "ep_datetime_test"
data_asset_name = "pypi_downloads"

# COMMAND ----------

context.sources.add_or_update_pandas(datasource_name)
datasource = context.get_datasource(datasource_name)
datasource.add_dataframe_asset(data_asset_name)
data_asset = datasource.get_asset(data_asset_name)
context.create_expectation_suite(expectation_suite_name)
expectation_suite = context.get_expectation_suite(expectation_suite_name)
batch_request = data_asset.build_batch_request(dataframe=df)
validator = context.get_validator(expectation_suite_name=expectation_suite_name, batch_request=batch_request)

# COMMAND ----------

validator.expect_column_values_to_be_between(
    column="download_time",
    min_value={"$PARAMETER": "datetime.datetime.now() - datetime.timedelta(weeks=12)"}, 
    strict_min=True
)

# COMMAND ----------

import datetime

validator.expect_column_values_to_be_between(
    column="download_time",
    min_value={"$PARAMETER": "now() - timedelta(weeks=12)"}, 
    strict_min=True
)

# COMMAND ----------

datetime.datetime.now() - datetime.timedelta(weeks=12)

# COMMAND ----------

df.columns

# COMMAND ----------



# COMMAND ----------


