# Databricks notebook source
# rm this cell before moving to prod

%load_ext autoreload
%autoreload 2

# clear notebook parameters
dbutils.widgets.removeAll()

# don't get data from bigquery
dev=True

# COMMAND ----------

# import custom utility modules
from utils import repo_utils, gx_utils
from utils.notebook_utils import Notebook

# other required imports
from datetime import datetime, timedelta
import pandas as pd
from pandas_gbq import read_gbq

# COMMAND ----------

# create a RepoConfig
# contains attributes for notebook, parameters, config, etc.
rc = repo_utils.get_repo_config("../config.yml")

# show attributes
rc.attributes

# COMMAND ----------

# test the agent
# ! export GX_CLOUD_ORGANIZATION_ID="3cd57c8a-611b-4393-a800-b633f0137c74" && export GX_CLOUD_ACCESS_TOKEN="b0a95ee1791d4049a680afca9a1a299b.V1.PXqsMypr8QkAmwr7RPHQzCPWwD7P6PEMIxTXtmRcRPQXXcOE-h7x6GjdJizkzw8PTfWneZJnWZMpq3UoXVqo_A" && export GX_CLOUD_BASE_URL="https://api.greatexpectations.io" && gx-agent

# COMMAND ----------

# get a date range for the data based on notebook params
# dates must be in ISO-8601 format!
# also specify the pypi package name of interest

date_range = [
    dbutils.widgets.get("param_dt_begin"),
    dbutils.widgets.get("param_dt_end"),
]

ts_range = [
    pd.Timestamp(date_range[0], tz="UTC"),
    pd.Timestamp(date_range[1], tz="UTC") + timedelta(hours=23, minutes=59, seconds=59),
]

pypi_pkg = dbutils.widgets.get("param_pypi_pkg")

print(
    f"Querying PyPI downloads of {pypi_pkg} with UTC timestamps between '{ts_range[0]}' and '{ts_range[1]}'."
)

# COMMAND ----------

# query to get data from public database
# must use BigQuery SQL syntax!
query = f"""
select
  timestamp as download_time,
  country_code,
  file.type as file_type,
  file.version as pkg_version,
  coalesce(details.python, details.implementation.version) as python_version,
  details.installer.name as installer_name,
  details.installer.version as installer_version,
  details.distro.name as distro_name,
  details.distro.version as distro_version,
  details.system.name as system_name,
  details.system.release as system_version,
  date(timestamp) as dt
from
  `bigquery-public-data.pypi.file_downloads`
where
  file.project = "{pypi_pkg}"
  and date(timestamp) between "{date_range[0]}"
  and "{date_range[1]}"
"""

df = read_gbq(query, use_bqstorage_api=True)

# COMMAND ----------

# create a checkpoint for validating pandas dataframe against expectation suite
checkpoint = gx_utils.default_checkpoint(pandas_df=df)

# COMMAND ----------

# run the checkpoint against the validator
checkpoint_run = checkpoint.run(
    batch_request=gx_utils.default_batch_request(pandas_df=df),
    evaluation_parameters={"min_ts": min(ts_range), "max_ts": max(ts_range)},
)

# COMMAND ----------

# create a results object
results = gx_utils.CheckpointRunResult(checkpoint_run)

# COMMAND ----------

# raise an error if expecations failed > expectations failures allowed
results.check_results(failures_allowed=0)

# COMMAND ----------

# list failures from validation results
results.list_failures()
