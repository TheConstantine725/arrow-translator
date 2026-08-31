"""
Data Type Definitions of cx_Oracle python driver mapped to Apache Arrow(PyArrow) data types
"""

import pyarrow as pa

DIALECT = "cx_Oracle"
TYPE_MAPS = {
    "DB_TYPE_NUMBER": pa.decimal128(38, 10),
    "DB_TYPE_BINARY_DOUBLE": pa.float64(),
    "DB_TYPE_BINARY_FLOAT": pa.float32(),
    "DB_TYPE_VARCHAR": pa.string(),
    "DB_TYPE_CHAR": pa.string(),
    "DB_TYPE_NVARCHAR": pa.string(),
    "DB_TYPE_DATE": pa.timestamp("us"),
    "DB_TYPE_TIMESTAMP": pa.timestamp("us"),
    "DB_TYPE_TIMESTAMP_TZ": pa.timestamp("us", tz="UTC"),
    "DB_TYPE_BLOB": pa.binary(),
    "DB_TYPE_CLOB": pa.string(),
    "DB_TYPE_BOOLEAN": pa.bool_(),
    "DB_TYPE_RAW": pa.binary(),
}
