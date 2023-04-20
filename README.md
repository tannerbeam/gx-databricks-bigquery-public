# gx-databricks-bigquery-public
> How to leverage the power of Databricks notebooks and GX data quality checks to create validated data workflows

Full instructions available at the [Great Expectations Blog](https://greatexpectations.io/blog/gx-databricks-notebooks-a-powerful-alliance-for-validated-data-workflows).


### Requirements

As this is a fairly detailed integration of multiple tools, some working knowledge of Python, SQL, Git, and Databricks is assumed. Prior experience with Great Expecations (GX) could prove useful, but is not strictly required.

- A Databricks account and a Workspace setup on a supported cloud provider (AWS, Azure, GCP).

- A compute cluster running [Databricks Runtime](https://docs.databricks.com/release-notes/runtime/releases.html) 12.0 or higher.

- A Git account with a [Git provider supported by Databricks](https://docs.databricks.com/repos/index.html).

- [Databricks for Git Repos](https://docs.databricks.com/repos/repos-setup.html) configured.

- A [Google Cloud Platform (GCP) project](https://cloud.google.com) account and ability to create a project and generate a service account with API credentials.


### Repo organization


#### `/` (top level)
- Directories 
- dotfiles (e.g. `.gitignore`)
-  GCP Service Account credentials JSON file
-  repo config YAML file
-  Pandas DataFrame PKL file with some sample data

#### `/src`
Databricks notebooks with executable code for scheduled orchestration

#### `/utils`
Python files (_not_ databricks notebooks!) to be imported

#### `/great_expectations` 
Anything pertaining to data validation with GX


### Repo configuration file

The default contents of the `config.yml` file are shown below:

```YAML
# assumed directory structure is: /Workspace/repos/{repo_directory}/{repo_name}

# {repo_directory} in assumed directory structure
repo_directory: "dev"

# {repo_name} in assumed directory structure
repo_name: "gx-databricks-bigquery-public"

# relative path of BigQuery service account credentials file
bigquery_creds_file: ".bigquery_service_account_creds.json"

# relative path of great expectations directory
gx_dir: "great_expectations"

# provide a name to help identify the GX data connector type
gx_connector_name: "pandas_fluent"

```

To avoid instances of `FileNotFoundError` and other problems, it's suggested to:
- Not rename or move the `config.yml` file out of the top-level directory.
- Only use one `key: 'value'` pair per line.
- Maintain a directory structure of `/Workspace/Repos/{repo_directory}/{repo_name}`. If you deviate from this pattern, the helper functions in the repo may not be able to locate files correctly.


### Links
- [Great Expectations](https://greatexpectations.io/)
- [Analyzing PyPI Package Downloads](https://packaging.python.org/en/latest/guides/analyzing-pypi-package-downloads/)
- [BigQuery API Authentication](https://cloud.google.com/bigquery/docs/authentication/)
- [pandas-gbq](https://pandas-gbq.readthedocs.io/en/latest/index.html)
- [BigQuery Syntax](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax)
- [BigQuery Public Datasets](https://cloud.google.com/bigquery/public-data)
- [Git Integration with Databricks Repos](https://docs.databricks.com/repos/index.html)
