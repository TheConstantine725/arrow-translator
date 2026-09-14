import pyarrow
from dotenv import dotenv_values
from sqlalchemy import create_engine, text

from arrow_translator import create_batch_generator

ENGINE = create_engine(dotenv_values(".env")["HANA"])
QUERY = """select * from saperp.mara"""


def main():
    yield from create_batch_generator(
        "some_name",
        ENGINE,
        QUERY,
    )


if __name__ == "__main__":
    for i, batch in enumerate(main()):
        print(batch)

    print("Done!!!")
