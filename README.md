# Arrow Translator
A small tool for querying data from databases using standard SQLAlchemy Engines and returns the underlying DBAPI Datatypes to Apache Arrow data types for more homogenised data pipelines

Available Dialects:
  * Postgres (psycopg2)
  * MySQL (PyMySQL, MySQL-Connector)
  * Microsoft SQL Server (pymssql, pyodbc) 
    * pyodbc requires from you that ODBC driver for MS SQL Server be install on the machine
  * OracledDB (cx_Oracle, oracledb)
    * Similar to pyodbc, oracledb might require the OracleDB driver's binaries or executables to be installed on the machine
  * SAP HANA (hdbcli)

# How to Use

## First Steps
Create an SQLAlchemy Engine and an SQL Query statement
```python
from sqlalchemy import create_engine
from arrow_translator import create_batch_generator

some_engine = create_engine(url = "dialect://hostname:port@user:password/dbname")

some_query = "select id, field, other_field from some_schema.some_table"
# Give your dataset some name
arrow_generator = create_batch_generator(name = "some_name", 
    engine = some_engine, 
    query = some_query)

for batch in arrow_generator:
  ...
```
The result will be something like that:

The result per batch will be something like that:
```
    id | field | other_field | _extraction_timestamp
    ...
```
When the extraction starts it add to each batch an extraction timestamp with the very creative name of _extraction_timestamp
This can help with the deduplication of duplicate records in case of a backfill to get the ones with the latest update

## Passing Bind Parameters
You can also add specific bind parameters in your query in case you want filter specific values.
```python
from sqlalchemy import create_engine
from arrow_translator import create_batch_generator

some_engine = create_engine(url = "dialect://hostname:port@user:password/dbname")

some_query = "select id, field, other_field from some_schema.some_table where field = :some_field"
# Create your bind parameters dictionary
some_bind_params = {"some_field": "foo"}
# Give your dataset some name
arrow_generator = create_batch_generator(name = "some_name", 
    engine = some_engine, 
    query = some_query
    bind_parms = some_bind_params)

for batch in arrow_generator:
  ...
```
This will create internally a text clause that looks something like that:
```sql
select * 
from some_schema.some_table 
where field = foo
```
Allowing you to perform more clinical data ingestion

## Enrich with Metadata
You can enrich the resulted RecordBatch with additional metadata that you can then handle on a downstream process

```python
from sqlalchemy import create_engine
from arrow_translator import create_batch_generator

some_engine = create_engine(url = "dialect://hostname:port@user:password/dbname")

some_query = "select id, field, other_field from some_schema.some_table"

# Create an dictionary with a field name as a string
# And a value that you wish pass
enrich = {"enrichment_field":0}

arrow_generator = create_batch_generator(name = "some_name", 
    engine = some_engine, 
    query = some_query,
    enrichment_map = enrich)

for batch in arrow_generator:
  ...
```
The result per batch will be something like that:
```markdown
    id | field | other_field | enrichment_field | _extraction_timestamp
    ...
```

## Remove Fields
You can remove fields from the resulted RecordBatch by passing a list of the fields you wish to remove from the resulted RecordBatch

```python
from sqlalchemy import create_engine
from arrow_translator import create_batch_generator

some_engine = create_engine(url = "dialect://user:password@hostname:port/dbname")

some_query = "select id, field, other_field from some_schema.some_table"

# Create a list with the column names you wish to exclude from your dataset
columns_for_removal = ["other_field"]

arrow_generator = create_batch_generator(name = "some_name", 
    engine = some_engine, 
    query = some_query,
    remove_columns = columns_for_removal
    )

for batch in arrow_generator:
  ...
```
The result per batch will be something like that:
```markdown
    id | field | _extraction_timestamp
    ...
```
