"""
OIDs of MySQL Python Connector mapped to Apache Arrow(PyArrow) data types
"""
import pyarrow as pa

DIALECT = "mysql-connector-python"
TYPE_MAPS = {  # same field type constants
        0: pa.decimal128(38, 10),
        1: pa.int8(),
        2: pa.int16(),
        3: pa.int32(),
        4: pa.float32(),
        5: pa.float64(),
        7: pa.timestamp("us"),
        8: pa.int64(),
        9: pa.int32(),
        10: pa.date32(),
        11: pa.time64("us"),
        12: pa.timestamp("us"),
        15: pa.string(),
        16: pa.binary(),
        252: pa.binary(),
        253: pa.string(),
        254: pa.string(),
    }
