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



__all__ = ["SQLTableDataSet", "SQLQueryDataSet"]

KNOWN_PIP_INSTALL = {
    "snowflake-connector-python": "snowflake-connector-python",
}

DRIVER_ERROR_MESSAGE = """
The snowflake-connector-python module/driver is missing when connecting to Snowflake. 
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






class SnowflakeQueryDataSet(AbstractDataSet[None, pd.DataFrame]):

    """``SnowflakeQueryDataSet`` loads data from a SQL table and saves a pandas
       """

    DEFAULT_LOAD_ARGS: Dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: Dict[str, Any] = {"index": False}
    # using Any because of Sphinx but it should be
    # sqlalchemy.engine.Engine or sqlalchemy.engine.base.Engine
    connections: Dict[str, Any] = {}

    
    

    def __init__(
        self,
        sql: str,
        credentials: Dict[str, Any],
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
    ) -> None:
        """Creates a new ``SnowflakeQueryDataSet``.
        """
        # -- should make this more robust
        if not sql:
            raise DataSetError("'sql' argument cannot be empty.")

        if not (credentials and "user" in credentials and credentials["user"]):
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

        self._load_args["sql"] = sql
        #self._save_args["name"] = table_name
        #self._filepath = None

        self._connection_creds = credentials

        self.create_connection(self._connection_creds)


    @classmethod
    def create_connection(cls, connection_kwargs: Dict[str, Any]) -> None:
        """Given a connection string, create singleton connection
        to be used across all instances of `SQLQueryDataSet` that
        need to connect to the same source.
        """
        if "current_connection" in cls.connections:
            return

        try:
            conn = snowflake.connector.connect(**connection_kwargs)

        except snowflake.connector.errors.DatabaseError as e:
            print(e)
            print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
            raise _get_missing_module_error(e) from e


        except snowflake.connector.errors.ProgrammingError as e:
            print(e)
            print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
            raise _get_missing_module_error(e) from e


        cls.connections["current_connection"] = conn

    @staticmethod
    def read_pandas_from_snowflake(connection: snowflake.connector, **load_args):
        """ To read data into a Pandas DataFrame, you use a
            Cursor to retrieve the data and then call one of 
            these Cursor methods to put the data into a Pandas
            DataFrame:"""

        try:
            #eventually add more options
            df = pd.read_sql(load_args['sql'], connection)

            return df
        
        except snowflake.connector.errors.ProgrammingError as e:
            print(e)
            print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
            return

        except Exception as e:
            print(e)
            print(f"query failed for {load_args['sql']}")
            return


    def _load(self) -> pd.DataFrame:
        conn = self.connections["current_connection"]  # type:ignore

        return SnowflakeQueryDataSet.read_pandas_from_snowflake(conn, **self._load_args)


    def _save(self, data: pd.DataFrame) -> None:
        """Saves image data to the specified filepath"""
        return

    def _describe(self) -> Dict[str, Any]:
        """Returns a dict that describes the attributes of the dataset"""

        return 









class SnowflakeTableDataSet(AbstractDataSet[pd.DataFrame, pd.DataFrame]):

    """``SnowflakeTableDataSet`` loads data from a SQL table and saves a pandas

    I think this should require a fully defined table name to make things explicit
    """

    DEFAULT_LOAD_ARGS: Dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: Dict[str, Any] = {"index": False}
    # using Any because of Sphinx but it should be
    # sqlalchemy.engine.Engine or sqlalchemy.engine.base.Engine
    connections: Dict[str, Any] = {}

    pd_to_sf_type_map = {"int": "int",
                     "int64": "int",
                    "object": "varchar(16777216)",
                    "datetime64[ns]": "datetime",
                    "float64": "float8",
                    "bool": "boolean",
                    "category": "varchar(16777216)",
                    "other": "varchar(16777216)"}

    
    def __init__(
        self,
        table_name: str,
        schema: str,
        database:str,
        credentials: Dict[str, Any],
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
    ) -> None:
        """Creates a new ``SnowflakeTableDataSet``.
        """
        # -- should make this more robust
        if not table_name:
            raise DataSetError("'table_name' argument cannot be empty.")
        if not schema:
            raise DataSetError("'schema' argument cannot be empty.")
        if not database:
            raise DataSetError("'database' argument cannot be empty.")

        if not (credentials and "user" in credentials and credentials["user"]):
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

        self._load_args["table_name"] = table_name
        self._save_args["table_name"] = table_name
        self._load_args["schema"] = schema
        self._save_args["schema"] = schema
        self._load_args["database"] = database
        self._save_args["database"] = database

        self._connection_creds = credentials

        self.create_connection(self._connection_creds)


    @classmethod
    def create_connection(cls, connection_kwargs: Dict[str, Any]) -> None:
        """Given a connection string, create singleton connection
        to be used across all instances of `SQLQueryDataSet` that
        need to connect to the same source.
        """
        if "current_connection" in cls.connections:
            return

        try:
            conn = snowflake.connector.connect(**connection_kwargs)

        except snowflake.connector.errors.DatabaseError as e:
            print(e)
            print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
            raise _get_missing_module_error(e) from e


        except snowflake.connector.errors.ProgrammingError as e:
            print(e)
            print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
            raise _get_missing_module_error(e) from e


        cls.connections["current_connection"] = conn


    @staticmethod
    def read_pandas_from_snowflake(connection: snowflake.connector, **load_args):
        """ To read data into a Pandas DataFrame, you use a
            Cursor to retrieve the data and then call one of 
            these Cursor methods to put the data into a Pandas
            DataFrame:"""

        sql = f""" SELECT * FROM "{load_args['database']}"."{load_args['schema']}"."{load_args['table_name']}" """
        try:
            #eventually add more options
            df = pd.read_sql(sql, connection)

            return df
        
        except snowflake.connector.errors.ProgrammingError as e:
            print(e)
            print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
            return

        except Exception as e:
            print(e)
            print(f"query failed for {sql}")
            return


    def _load(self) -> pd.DataFrame:
        conn = self.connections["current_connection"]

        return SnowflakeTableDataSet.read_pandas_from_snowflake(conn, **self._load_args)


    def _save(self, data: pd.DataFrame) -> None:
        """Saves data back to a Snowflake table"""
        conn = self.connections["current_connection"]

        create_statements = self.create_table_sql_statements(data, **self._save_args)

        status = self.write_table(create_statements, conn, data, **self._save_args)

        return status

    def _describe(self) -> Dict[str, Any]:
        """Returns a dict that describes the attributes of the dataset"""

        return 


    @staticmethod
    def convert_to_snowflake_safe_names(name:str) -> str:
        """ Make sure names are Snowflake Safe.
            Special characters aren't allowed in Snowflake """

        safe_name = re.sub(r'\W+', '', name)
        return safe_name


    def create_table_sql_statements(self, df:pd.DataFrame, pd_to_sf_type_map: Dict[str, str] = pd_to_sf_type_map, **save_args) -> list:

        # construct columns + types
        columns_text_list = []
        for column in df.columns:
            sf_type = pd_to_sf_type_map[df[column].dtype.name]

            column_name_preped = SnowflakeTableDataSet.convert_to_snowflake_safe_names(column)
            column_text = f""" "{column_name_preped}" {sf_type} """
            columns_text_list.append(column_text)

        table = SnowflakeTableDataSet.convert_to_snowflake_safe_names(save_args['table_name'])
        columns_text = ', '.join(columns_text_list)
        
        create_db_statment = f""" CREATE DATABASE IF NOT EXISTS "{save_args['database']}" """
        create_schema_statment = f""" CREATE SCHEMA IF NOT EXISTS "{save_args['database']}"."{save_args['schema']}" """

        if save_args['if_exists'] == 'replace':
            create_tbl_statement = f""" CREATE OR REPLACE TABLE "{save_args['database']}"."{save_args['schema']}"."{table}" ( {columns_text} ) """
        else:
            create_tbl_statement = f""" CREATE IF NOT EXISTS "{save_args['database']}"."{save_args['schema']}"."{table}" ( {columns_text} ) """

        create_statements = [create_db_statment, create_schema_statment, create_tbl_statement] 

        return create_statements

    
    def write_table(self, create_statements:list, conn:snowflake.connector, df:pd.DataFrame, **save_args) -> bool:
        #Create the table if it doesn't exist
        for statement in create_statements:
            conn.cursor().execute(statement)
        
        conn.cursor().execute(f""" use database "{save_args['database']}" """)
        conn.cursor().execute(f""" use schema "{save_args['database']}"."{save_args['schema']}" """)

        #prep table name again
        table = SnowflakeTableDataSet.convert_to_snowflake_safe_names(save_args['table_name'])

        #prep column names
        columns = df.columns
        columns = [ SnowflakeTableDataSet.convert_to_snowflake_safe_names(column) for column in columns]
        df.columns = columns

        # Loading our DataFrame data to the newly created empty table
        success, num_chunks, num_rows, output = write_pandas(
                conn=conn,
                df=df,
                table_name = table
            )
        
        return success

