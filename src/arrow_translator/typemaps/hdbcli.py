"""
OIDs for SAP HANA' s hdbcli data types mapped to Apache Arrow(Pyarrow) data types
"""

import pyarrow as pa


DIALECT = "hdbcli"
TYPE_MAPS = {
    # --- hdbcli (SAP HANA) type codes ---
    # Integer type codes from hdbcli / SAP HANA column store type system
    1: pa.int8(),  # TINYINT
    2: pa.int16(),  # SMALLINT
    3: pa.int32(),  # INTEGER
    4: pa.int64(),  # BIGINT
    5: pa.decimal128(38, 10),  # DECIMAL / FIXED
    6: pa.float32(),  # REAL / FLOAT(p ≤ 24)
    7: pa.float64(),  # DOUBLE / FLOAT
    8: pa.string(),  # CHAR
    9: pa.string(),  # VARCHAR
    10: pa.string(),  # NCHAR
    11: pa.string(),  # NVARCHAR
    12: pa.binary(),  # BINARY
    13: pa.binary(),  # VARBINARY
    14: pa.date32(),  # DATE
    15: pa.time64("us"),  # TIME
    16: pa.timestamp("us"),  # TIMESTAMP
    25: pa.string(),  # CLOB
    26: pa.string(),  # NCLOB
    27: pa.binary(),  # BLOB
    28: pa.bool_(),  # BOOLEAN
    29: pa.string(),  # STRING (HANA-internal)
    30: pa.string(),  # NSTRING (HANA-internal)
    47: pa.decimal128(38, 10),  # SMALLDECIMAL
    51: pa.string(),  # TEXT (full-text index type)
    52: pa.string(),  # SHORTTEXT
    55: pa.binary(),  # BINTEXT
    62: pa.timestamp("us"),  # SECONDDATE (date+time, no sub-second)
}
