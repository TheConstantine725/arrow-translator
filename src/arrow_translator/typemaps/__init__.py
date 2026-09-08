from typing import Any

# ================== ADD DIALECTS HERE =======================================================
from .cx_Oracle import DIALECT as CX_ORACLE_DIALECT
from .cx_Oracle import TYPE_MAPS as CX_ORACLE_TYPES
from .hdbcli import DIALECT as HDBCLI_DIALECT
from .hdbcli import TYPE_MAPS as HDBCLI_TYPES
from .mysql_connector_python import (
    DIALECT as MYSQL_CONN_DIALECT,
)
from .mysql_connector_python import (
    TYPE_MAPS as MYSQL_CONN_TYPES,
)
from .oracledb import DIALECT as ORCL_DIALECT
from .oracledb import TYPE_MAPS as ORCL_TYPES
from .psycopg2 import DIALECT as PSYCOPG2_DIALECT
from .psycopg2 import TYPE_MAPS as PSYCOPG2_TYPES
from .pymssql import DIALECT as MSSQL_DIALECT
from .pymssql import TYPE_MAPS as MSSQL_TYPES
from .pymysql import DIALECT as PYMYSQL_DIALECT
from .pymysql import TYPE_MAPS as PYMYSQL_TYPES
from .pyodbc import DIALECT as PYODBC_DIALECT
from .pyodbc import TYPE_MAPS as PYODBC_TYPES


# ================== ADD DIALECTS HERE: END =======================================================
class Lexicon:
    typemaps = {
        PSYCOPG2_DIALECT: PSYCOPG2_TYPES,
        PYMYSQL_DIALECT: PYMYSQL_TYPES,
        MYSQL_CONN_DIALECT: MYSQL_CONN_TYPES,
        MSSQL_DIALECT: MSSQL_TYPES,
        PYODBC_DIALECT: PYODBC_TYPES,
        CX_ORACLE_DIALECT: CX_ORACLE_TYPES,
        ORCL_DIALECT: ORCL_TYPES,
        HDBCLI_DIALECT: HDBCLI_TYPES,
    }

    @classmethod
    def to_pyarrow(cls, dialect: str, oid: Any):
        return cls.typemaps[dialect][oid]

    @property
    @classmethod
    def available_drivers(cls):
        return cls.typemaps.keys()
