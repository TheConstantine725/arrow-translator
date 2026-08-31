"""
OIDs of PyMySql Python DBAPI Driver mapped to Apache Arrow(PyArrow) data types
"""

import pyarrow as pa

DIALECT = "pymysql"
TYPE_MAPS = {
    0: pa.decimal128(38, 10),  # DECIMAL
    1: pa.int8(),  # TINY
    2: pa.int16(),  # SHORT
    3: pa.int32(),  # LONG
    4: pa.float32(),  # FLOAT
    5: pa.float64(),  # DOUBLE
    7: pa.timestamp("us"),  # TIMESTAMP
    8: pa.int64(),  # LONGLONG
    9: pa.int32(),  # INT24
    10: pa.date32(),  # DATE
    11: pa.time64("us"),  # TIME
    12: pa.timestamp("us"),  # DATETIME
    15: pa.string(),  # VARCHAR
    16: pa.binary(),  # BIT
    252: pa.binary(),  # BLOB
    253: pa.string(),  # VAR_STRING
    254: pa.string(),  # STRING
}
