from time import sleep

from dotenv import dotenv_values
from sqlalchemy import create_engine

from arrow_translator import create_batch_generator, create_record_batch_reader

ENGINE = create_engine(dotenv_values(".env")["SAP_HANA"])
QUERY = """select * from saperp.mara"""


def test_1():
    for i in create_record_batch_reader("some_name", ENGINE, QUERY, batch_size=10_000):
        print(i)
        print()
    print("=" * 100)
    sleep(5)
    for i in create_batch_generator(
        "some_other_name", ENGINE, QUERY, batch_size=10_000
    ):
        print(i)
        print()


if __name__ == "__main__":
    test_1()
