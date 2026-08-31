"""
OIDs of PostgreSQL psycopg2 driver mapped to Apache Arrow (PyArrow) datatypes
"""

import pyarrow as pa

DIALECT = "psycopg2"
TYPE_MAPS = {
    16: pa.bool_(),  # BOOL
    17: pa.binary(),  # BYTEA
    20: pa.int64(),  # INT8
    21: pa.int16(),  # INT2
    23: pa.int32(),  # INT4
    25: pa.string(),  # TEXT
    114: pa.string(),  # JSON
    700: pa.float32(),  # FLOAT4
    701: pa.float64(),  # FLOAT8
    1042: pa.string(),  # BPCHAR
    1043: pa.string(),  # VARCHAR
    1082: pa.date32(),  # DATE
    1083: pa.time64("us"),  # TIME
    1114: pa.timestamp("us"),  # TIMESTAMP
    1184: pa.timestamp("us", tz="UTC"),  # TIMESTAMPTZ
    1700: pa.decimal128(38, 10),  # NUMERIC
    2950: pa.string(),  # UUID
}
