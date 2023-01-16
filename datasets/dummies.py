

# a dataset to pass table names as datasets for working with snowflake
# -- this is all that is needed in snowflake

# For local dev it would be nice to have a subset of the dataset

# so maybe it should return a tuple 
# (table name, data object) <- where the data object is a snowpark dataframe or pandas dataframe, or session?

# for more complex object we'll also need a stage pointer

# it all needs to be locaiton specific

"""  Starting from - ``SQLDataSet`` to load and save data to a SQL backend."""

import copy
import re
from pathlib import PurePosixPath
from typing import Any, Dict, NoReturn, Optional
import re

import fsspec
import pandas as pd

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


# from sqlalchemy import create_engine
# from sqlalchemy.exc import NoSuchModuleError

from kedro.io.core import (
    AbstractDataSet,
    DataSetError,
    get_filepath_str,
    get_protocol_and_path,
)



class TableNameDataSet(AbstractDataSet[None, pd.DataFrame]):

    """`TableNameDataSet``use table name as a dummy"""
    DEFAULT_LOAD_ARGS: Dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: Dict[str, Any] = {}

    def __init__(
        self,
        table_name: str,
        schema: str,
        database:str,
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
    ) -> None:
        """Creates a new ``SnowflakeQueryDataSet``.
        """
        if not table_name:
            raise DataSetError("'table_name' argument cannot be empty.")
        if not schema:
            raise DataSetError("'schema' argument cannot be empty.")
        if not database:
            raise DataSetError("'database' argument cannot be empty.")

        # Handle default load and save arguments
        self._load_args = copy.deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        self._save_args = copy.deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

        self._load_args["table_name"] = table_name
        self._save_args["table_name"] = table_name
        self._load_args["schema"] = schema
        self._save_args["schema"] = schema
        self._load_args["database"] = database
        self._save_args["database"] = database


    def _load(self) -> pd.DataFrame:

        return f"""{self._load_args["database"]}.{self._load_args["schema"]}.{self._load_args["table_name"]}"""


    def _save(self, data: pd.DataFrame) -> None:

        return f"""{self._save_args["database"]}.{self._save_args["schema"]}.{self._save_args["table_name"]}"""


    def _describe(self) -> Dict[str, Any]:
        """Returns a dict that describes the attributes of the dataset"""

        return 


class SprocNameDataSet(AbstractDataSet[None, pd.DataFrame]):

    """`TableNameDataSet``use table name as a dummy"""
    DEFAULT_LOAD_ARGS: Dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: Dict[str, Any] = {}

    def __init__(
        self,
        sproc_name: str,
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
    ) -> None:
        """Creates a new ``SnowflakeQueryDataSet``.
        """
        if not sproc_name:
            raise DataSetError("'sproc_name' argument cannot be empty.")

        # Handle default load and save arguments
        self._load_args = copy.deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        self._save_args = copy.deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

        self._load_args["sproc_name"] = sproc_name
        self._save_args["sproc_name"] = sproc_name

    def _load(self) -> pd.DataFrame:

        return f"""{self._load_args["sproc_name"]} """


    def _save(self, data: pd.DataFrame) -> None:

        return f"""{self._save_args["sproc_name"]}"""


    def _describe(self) -> Dict[str, Any]:
        """Returns a dict that describes the attributes of the dataset"""

        return 

