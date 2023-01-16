import copy
import re
from pathlib import PurePosixPath
from typing import Any, Dict, NoReturn, Optional, Callable
import re

import fsspec
import pandas as pd

#import snowflake.connector
#from snowflake.connector.pandas_tools import write_pandas
from snowflake.snowpark import Session
import snowflake.snowpark as sp


# from sqlalchemy import create_engine
# from sqlalchemy.exc import NoSuchModuleError

from kedro.io.core import (
    AbstractDataSet,
    DataSetError,
    get_filepath_str,
    get_protocol_and_path,
)


# needed inputs example:
# connection_parameters = {
# "account": "gib50850",
# "user": "prestontrial5",
# "password": "",
# "role": "ACCOUNTADMIN",  # optional
# "warehouse": "COMPUTE_WH",  # optional
# # "database": "<your snowflake database>",  # optional
# # "schema": "<your snowflake schema>",  # optional
# }  

# session = Session.builder.configs(connection_parameters).create()  



__all__ = ["SnowparkSessionDataSet"]

KNOWN_PIP_INSTALL = {
    "snowflake-snowpark-python": "snowflake-snowpark-python",
}

DRIVER_ERROR_MESSAGE = """
The snowflake-snowpark-python module/driver is missing when connecting to Snowflake. 
Make sure that the [pandas] tools are installed for the Snowflake connectort: \n
``pip install "snowflake-connector-python[pandas]"``
\n\n
"""


def _find_known_drivers(module_import_error: ImportError) -> Optional[str]:
    """Looks up known keywords in a ``ModuleNotFoundError`` so that it can
    provide better guideline for the user.

    Args:
        module_import_error: Error raised while connecting to a SQL server.

    Returns:
        Instructions for installing missing driver. An empty string is
        returned in case error is related to an unknown driver.

    """

    # module errors contain string "No module name 'module_name'"
    # we are trying to extract module_name surrounded by quotes here
    res = re.findall(r"'(.*?)'", str(module_import_error.args[0]).lower())

    # in case module import error does not match our expected pattern
    # we have no recommendation
    if not res:
        return None

    missing_module = res[0]

    if KNOWN_PIP_INSTALL.get(missing_module):
        return (
            f"You can also try installing missing driver with\n"
            f"\npip install {KNOWN_PIP_INSTALL.get(missing_module)}"
        )

    return None


def _get_missing_module_error(import_error: ImportError) -> DataSetError:
    missing_module_instruction = _find_known_drivers(import_error)

    if missing_module_instruction is None:
        return DataSetError(
            f"{DRIVER_ERROR_MESSAGE}Loading failed with error:\n\n{str(import_error)}"
        )

    return DataSetError(f"{DRIVER_ERROR_MESSAGE}{missing_module_instruction}")


def _get_snowflake_sql_missing_error() -> DataSetError:
    return DataSetError(
        "The SQL dialect in your connection is not supported by "
        "the Snowflake Connector. Please refer to "
        "   "
        "for more information."
    )





class SnowparkSessionDataSet(AbstractDataSet[None, Callable]):

    """ `SnowparkSessionDataSet` returns snowpark session object"""
    DEFAULT_LOAD_ARGS: Dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: Dict[str, Any] = {"index": False}
    # using Any because of Sphinx but it should be
    # sqlalchemy.engine.Engine or sqlalchemy.engine.base.Engine
    sessions: Dict[str, Any] = {}

    def __init__(
        self,
        credentials: Dict[str, Any],
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None, 
    ) -> None:

        if not (credentials and "user" in credentials and credentials['user']):
            raise DataSetError(
                "'user', 'password', and 'account' must be passed"
                " see docs for other connection methods"
            )

        # Handle default load and save arguments
        self._load_args = copy.deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        self._save_args = copy.deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

        self._session_creds = credentials
        self.create_session(self._session_creds)

    @classmethod
    def create_session(cls, session_kwargs: Dict[str, Any]) -> None:
        """
            Given a connection string create a singleton session
            to be used accross all instances of `SnowparkSessionDataSet`
        """
        if "current_session" in cls.sessions:
            return
        
        # try: 
        session = Session.builder.configs(session_kwargs).create() 
        
        # except Exception as e:
        #     print(e)
        
        cls.sessions['current_session'] = session

    def _load(self) -> Callable:
        session = self.sessions['current_session']

        return session

    
    def _save(self) -> None:
        """ Not Used """
        return

    def _describe(self) -> None:
        """ return dict that describes attr of the dataset (not used)"""
        return