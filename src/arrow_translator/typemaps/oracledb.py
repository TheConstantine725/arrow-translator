"""
Data Type Definitions of OracleDB (Oracle DB) python library to Apache Arrow(PyArrow) data types
"""
# import oracledb as orcl
import pyarrow as pa

DIALECT = "oracledb"
TYPE_MAPS = {  # modern Oracle driver, same type names
    orcl.DB_TYPE_NUMBER: pa.decimal128(38, 10),
    orcl.DB_TYPE_BINARY_DOUBLE: pa.float64(),
    orcl.DB_TYPE_BINARY_FLOAT: pa.float32(),
    orcl.DB_TYPE_VARCHAR: pa.string(),
    orcl.DB_TYPE_CHAR: pa.string(),
    orcl.DB_TYPE_NVARCHAR: pa.string(),
    orcl.DB_TYPE_DATE: pa.timestamp("us"),
    orcl.DB_TYPE_TIMESTAMP: pa.timestamp("us"),
    orcl.DB_TYPE_TIMESTAMP_TZ: pa.timestamp("us", tz="UTC"),
    orcl.DB_TYPE_BLOB: pa.binary(),
    orcl.DB_TYPE_CLOB: pa.string(),
    orcl.DB_TYPE_BOOLEAN: pa.bool_(),
    orcl.DB_TYPE_RAW: pa.binary(),
}
