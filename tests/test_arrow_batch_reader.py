from hashlib import sha256
from time import sleep

from dotenv import dotenv_values
from sqlalchemy import create_engine

from arrow_translator import create_record_batch_reader

ENGINE = create_engine(dotenv_values(".env")["SAP_HANA"])
QUERY = """select * from saperp.vbrp where 1 = 1
and prsdt >= '20260101' and prsdt < '20260105'
"""


def test_1():
    # if (_log_path := Path.cwd().joinpath(FILE_LOGGER)).exists():
    #     _log_path.unlink(True)

    total_rows = 0
    print("=" * 100)
    for _, i in enumerate(
        create_record_batch_reader(
            "some_name",
            ENGINE,
            QUERY,
            batch_size=6_000,
            enrichment_map={
                "_snapshot_id": 0,
                "_pipeline_name": "some_pipeline_name",
                "_pipeline_hash": sha256(b"some_pipeline_name").hexdigest(),
            },
            remove_columns=["mandt", "campaign"],
        )
    ):
        batch_number = _ + 1
        total_rows += i.num_rows
        print(f"Extracted batch {batch_number} with {i.num_rows} of {total_rows}")

    print("=" * 100)
    sleep(1)


if __name__ == "__main__":
    test_1()
