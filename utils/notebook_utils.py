from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from typing import Optional, Union
from dataclasses import dataclass
from ruamel.yaml import YAML
from datetime import datetime, timedelta
import json
import re
import os

# get existing Spark context
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


@dataclass
class NbParam:
    """
    Dataclass for a Databricks notebook parameter
    """

    name: str
    default_value: str
    choices: Optional[list[Union[str, int]]] = None
    dropdown: bool = False

    def __post_init__(self):
        self.dbutils = DBUtils(SparkSession.builder.getOrCreate())
        self.widget = self.create_widget()

    def create_widget(self):
        if not self.dropdown:
            return self.dbutils.widgets.text(
                name=self.name, defaultValue=self.default_value
            )
        else:
            if not self.default_value in self.choices:
                self.choices.append(self.default_value)

            return self.dbutils.widgets.dropdown(
                name=self.name, defaultValue=self.default_value, choices=self.choices
            )

    def remove_widget(self):
        return self.dbutils.widgets.remove(self.name)

    def get_value(self):
        return self.dbutils.widgets.get(self.name)


def get_widgets(widgets: list[tuple]) -> list[str]:
    w_names: list = []
    for tup in widgets:
        if len(tup) == 2:
            w = NbParam(name=tup[0], default_value=tup[1])
        else:
            w = NbParam(
                name=tup[0], default_value=tup[1], choices=tup[2], dropdown=True
            )

        w_names.append(w.name)

    return w_names


def get_nb_params_from_dict(repo_vars: dict) -> dict:
    """
    Create nb widgets from relevant param_ vars in a dict
    """

    params: list(tuple) = [
        (k, v) for k, v in repo_vars.items() if k.startswith("param_")
    ]

    # clear existing widget parameters
    dbutils.widgets.removeAll()

    get_widgets(params)

    if len(params) == 0:
        return {}
    else:
        return {p[0]: p[1] for p in params}


class DatabricksRuntimeException(Exception):
    """
    Exception to fail notebook if DBR <= major version 12.
    """

    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = None

    def __str__(self):
        if self.message:
            return "DatabricksRuntime: {0} ".format(self.message)
        else:
            return "DatabricksRuntimeException"


@dataclass
class Notebook:
    """
    Class for databricks notebook.
    """

    def __post_init__(self):
        self.dbutils = DBUtils(SparkSession.builder.getOrCreate())
        self.path = os.getcwd()
        self.context = self.get_context()
        self.url = self.get_url()
        self.has_git = self.check_for_git()
        self.name = self.get_name()
        self.repo = self.get_repo()
        self.branch = self.get_branch()
        self.config_file = find_config_file()
        self.runtime_version = self.get_runtime_version()
        self.valid_runtime = self.valid_runtime()
        self.attributes: dict[str] = {
            k: v for k, v in self.__dict__.items() if not k in ["dbutils", "context"]
        }

    def get_context(self) -> dict[str]:
        return json.loads(
            self.dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .toJson()
        )

    def get_url(self) -> str:
        host = self.context.get("tags").get("browserHostName")
        if host is None:
            return None
        else:
            org_id = self.context["tags"]["orgId"]
            nb_id = self.context["tags"]["notebookId"]
            return f"https://{host}/?o={org_id}#notebook/{nb_id}"

    def check_for_git(self) -> bool:
        rel_path = self.context.get("extraContext").get("mlflowGitRelativePath")
        return True if rel_path is not None else False

    def get_name(self) -> str:
        if not self.has_git:
            path = self.context.get("extraContext").get("notebook_path")
            name = path.split("/")[-1]
        else:
            dirname = os.getcwd().split("/")[-1]
            rel_path = self.context.get("extraContext").get("mlflowGitRelativePath")
            name = re.search(rf"{dirname}\/(\w+)$", rel_path).group(1)
        name = name.replace("nb_", "") if name.startswith("nb_") else name
        return name

    def get_repo(self) -> Union[str, None]:
        return self.context.get("extraContext").get("mlflowGitUrl")

    def get_branch(self) -> Union[str, None]:
        return self.context.get("extraContext").get("mlflowGitReference")

    def get_runtime_version(self) -> Union[str, None]:
        pat = r"(^\d{1,2}\.\d{1,2}).*"
        context = self.get_context()
        full_version = context.get("tags").get("sparkVersion")
        return re.search(pat, full_version).group(1)

    def valid_runtime(self) -> bool:
        pat = r"(^\d{1,2})\.\d{1,2}"
        major_version = re.search(pat, self.runtime_version).group(1)

        if not major_version:
            print(
                "Unknown Databricks Runtime version. Please note that this repo requires a cluster running Databricks Runtime 12.0 or higher."
            )
            return False

        if int(major_version) < 12:
            raise DatabricksRuntimeException(
                f"This repo requires a cluster running Databricks Runtime 12.0 or higher but your runtime is {self.runtime_version}."
            )
            return False
        
        else:
            return True


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


def default_query_date() -> str:
    """
    Default date as %Y-%m-%d string for querying data
    """
    return (datetime.today().date() - timedelta(days=1)).strftime("%Y-%m-%d")


def read_json(path: str):
    """
    Load json file from path
    """
    with open(path, "r") as f:
        return json.load(f)


def read_yaml(path: str):
    """
    Load yaml file from path
    """
    yaml = YAML()
    with open(path, "r") as f:
        return yaml.load(f)