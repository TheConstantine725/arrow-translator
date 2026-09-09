import pyarrow as pa
from dotenv import dotenv_values
from sqlalchemy import create_engine

from arrow_translator import create_batch_generator, create_record_batch_reader

QUERY = "select * from ingestion_ducklake_v04.ducklake_column"
ENGINE = create_engine(dotenv_values(".env")["URL"])


def main():
    batch_reader = create_record_batch_reader("ducks", ENGINE, QUERY, batch_size=10)
    for i in batch_reader:
        print(i)


if __name__ == "__main__":
    main()
