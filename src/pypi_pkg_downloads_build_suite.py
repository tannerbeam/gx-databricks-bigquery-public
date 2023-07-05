# Databricks notebook source
# rm this cell before moving to prod
%load_ext autoreload
%autoreload 2

# clear notebook parameters
dbutils.widgets.removeAll()

# COMMAND ----------

# import custom utility modules
from utils import repo_utils, gx_utils

# other required imports
from datetime import datetime, timedelta
import pandas as pd

# COMMAND ----------

# create a RepoConfig
# contains attributes for notebook, parameters, config, etc.
rc = repo_utils.get_repo_config("../config.yml")
                                
# show attributes
rc.attributes

# COMMAND ----------

# get sample data for building the suite
df=pd.read_pickle("../pypi_sample_data_great-expectations.pkl")

# COMMAND ----------

# use different than default names 
# default names created from notebook filename

datasource_name = "pypi_pkg_downloads_pandas_runtime"
expectation_suite_name = "pypi_pkg_downloads"
asset_name = "pypi_pkg_downloads"

# COMMAND ----------

# default context
context = gx_utils.default_context()

# default datasource
datasource = gx_utils.default_datasource(datasource_name=datasource_name)

# default expectation suite
expectation_suite = gx_utils.default_expectation_suite(
    expectation_suite_name=expectation_suite_name
)

# use sample data in repo for creating validation rules
df = pd.read_pickle("../pypi_sample_data_great-expectations.pkl")

# COMMAND ----------

# add asset to datasource if not exists
if not asset_name in list(datasource.get_asset_names()):
    datasource.add_dataframe_asset(asset_name)

datasource = context.get_datasource(datasource_name)
asset = datasource.get_asset(asset_name)
asset.dataframe = df

batch_request = asset.build_batch_request()

base_validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name=expectation_suite.name
)

validator = gx_utils.default_validations(df, base_validator)
