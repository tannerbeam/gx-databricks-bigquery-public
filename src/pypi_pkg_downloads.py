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

# create a Notebook
# contains attributes for notebook, filestystem, git, etc.
nb = Notebook()

# show attributes
nb.attributes

# COMMAND ----------

# create a RepoConfig
# contains attributes for notebook, parameters, config, etc.
rc = repo_utils.get_repo_config(config_file=nb.config_file)

# show attributes
rc.attributes

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

# COMMAND ----------

if dev: 
    df=pd.read_pickle("../pypi_sample_data_great-expectations.pkl")
else:
    # use pandas_gbq library to get a pandas dataframe of query results
    df = read_gbq(query, use_bqstorage_api=True)

# COMMAND ----------

context = gx_utils.default_context()
data_asset = gx_utils.default_asset(pandas_df = df)
data_asset.dataframe = df
batch_request = data_asset.build_batch_request()

# COMMAND ----------

data_asset.get_batch_list_from_batch_request(batch_request)

# COMMAND ----------

validator = context.get_validator(
    batch_request=batch_request, 
    expectation_suite_name=rc.expectation_suite_name
)

# COMMAND ----------

validator = context.get_validator(expectation_suite_name=rc.expectation_suite_name, batch_request=batch_request)

# COMMAND ----------

# get a gx validator using the expectation suite in RepoConfig
# if overwrite=True:
# - a new validator object will be created using default validaiton rules
# - the expecation suite on disk will be overwritten
# if overwrite=False: 
# - returns expectation suite from disk if exists
# - DataContextError if expectation suite does not exist
# -- change to overwrite=True to create suite for first time
# -- subsequently can use overwrite=False

validator = gx_utils.default_validator(
    pandas_df=df,  
    date_range=date_range, 
    batch_request=batch_request,
    overwrite=True
)

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
