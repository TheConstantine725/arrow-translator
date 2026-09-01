# Arrow Translator
A small tool for querying data from databases using standard SQLAlchemy Engines and returns the underlying DBAPI Datatypes to Apache Arrow data types for more homogenised data pipelines

Available Dialects:
  * Postgres (psycopg2)
  * MySQL (PyMySQL, MySQL-Connector)
  * Microsoft SQL Server (pymssql, pyodbc) 
    * pyodbc requires from you that ODBC driver for MS SQL Server be install on the machine
  * OracledDB (cx_Oracle, oracledb)
    * Similar to pyodbc, oracledb requires the OracleDB driver's binaries or executables to be installed on the machine
  * SAP HANA (hdbcli)
