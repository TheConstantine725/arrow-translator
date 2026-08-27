"""
OIDs for ODBC Driver of Microsoft SQL Server mapped to Apache Arrow(PyArrow) datatypes
"""

import pyarrow as pa

DIALECT = "pyodbc"
TYPE_MAPS =  {
        bool: pa.bool_(),
        int: pa.int64(),
        float: pa.float64(),
        str: pa.string(),
        bytes: pa.binary(),
        "datetime": pa.timestamp("us"),
        "date": pa.date32(),
        "time": pa.time64("us"),
        "decimal": pa.decimal128(38, 10),
    }
