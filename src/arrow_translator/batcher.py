import datetime
import time
from collections.abc import Generator, Iterator, Sequence
from typing import Any, Self, final

import numpy as np
import pyarrow as pa
from sqlalchemy import Engine, Row, TextClause, text

from .descriptor import create_arrow_schema


class Batch:
    """
    A high level abstraction of the Arrow Record Batch that allows for fast additions or removals of field
    during extraction.
    Provides properties for more effective logging.
    """

    def __init__(self, batch: pa.RecordBatch):
        self.batch = batch

    @property
    def size(self) -> int:
        return self.batch.nbytes

    @property
    def column_name(
        self,
    ) -> list[str]:
        return self.batch.column_names

    @property
    def num_of_columns(
        self,
    ) -> int:
        return self.batch.num_columns

    @property
    def num_of_rows(self) -> int:
        return self.batch.num_rows

    @property
    def schema(self) -> pa.Schema:
        return self.batch.schema

    def remove_columns(self, columns: str | list[str]) -> Self:
        self.batch = self.batch.drop_columns(columns)
        return self

    def append_columns(self, value_mapping: dict[str, Any]) -> Self:
        for column_name, value in value_mapping.items():
            array_of_value = pa.array(
                [value for _ in range(self.num_of_rows)],
            )
            self.batch = self.batch.append_column(column_name, array_of_value)
        return self

    @final
    def collect(self) -> pa.RecordBatch:
        return self.batch


class ArrowBatchReader:
    """
    A small builder class. It works as the interface to collect the data using SQLAlchemy then translates
    the DBAPI cursor's metadata to valid Arrow Data Types.
    Helping to query the data more flexibly and in a streaming way.
    """

    def __init__(self, name: str):
        self.name: str = name
        self.extraction_timestamp = datetime.datetime.now().astimezone()
        self.engine: Engine | None = None
        self.query: TextClause | None = None
        self.columns_to_remove: list[str] | None = None
        self.columns_for_enrichment: dict[str, Any] | None = None
        self.arrow_schema: pa.Schema | None = None

    def add_columns_for_removal(self, columns: str | list[str] | None = None) -> Self:
        if columns is not None:
            self.columns_to_remove = []
            if isinstance(columns, str):
                self.columns_to_remove.append(columns)
            elif isinstance(columns, list):
                self.columns_to_remove.extend(columns)
        return self

    def add_columns_for_enrichment(
        self, value_map: dict[str, Any] | None = None
    ) -> Self:
        self.columns_for_enrichment = value_map
        return self

    def with_engine(self, engine: Engine):
        self.engine = engine
        return self

    def with_query(
        self, query: str | TextClause, bind_parameters: dict[str, Any] | None = None
    ) -> Self:
        if isinstance(query, str):
            try:
                temp_query = text(query)
            except Exception as error:
                print(
                    f"Error in the transformation of the string to a query for the dataset with the name {self.name}"
                )
                print(error)
            else:
                if bind_parameters is None:
                    self.query = temp_query
                else:
                    try:
                        result = temp_query.bindparams(**bind_parameters)
                    except Exception as error:
                        print(
                            f"Error when attempting to attach bind parameters to the query of the dataset {self.name}"
                        )
                        print(error)
                    else:
                        self.query = result
        else:
            self.query = query
        return self

    def _cursor_result_transposition(self, rows: Sequence[Row[Any]]):
        try:
            result = np.array([tuple(row) for row in rows], dtype=object).transpose()
        except Exception as error:
            print(f"Error in the transposition of the dataset {self.name}")
            print(error)
            raise error
        else:
            return result

    def _transposed_arrow_arrays(
        self,
        transposed_data: np.ndarray[Any],
    ):
        result_arrow_arrays: list[pa.Array] = []
        try:
            for column in transposed_data:
                result_arrow_arrays.append(pa.array(column))
        except Exception as error:
            print("Error in the transformation of the numpy array to Arrow array")
            print(error)
        else:
            return result_arrow_arrays

    def _arrow_arrays_to_batch(self, arrow_arrays: list[pa.Array], schema: pa.Schema):
        try:
            result_record_batch = pa.record_batch(data=arrow_arrays, schema=schema)
        except Exception as error:
            print(
                f"Error in the transformation of the arrow arrays to a RecordBatch for resource {self.name}"
            )
            print(error)
        else:
            return Batch(result_record_batch)

    def _compile_batch(
        self, cursor_result: Sequence[Row[Any]], arrow_schema: pa.Schema
    ):
        transposed = self._cursor_result_transposition(cursor_result)
        arrow_arrays = self._transposed_arrow_arrays(transposed)
        batch = self._arrow_arrays_to_batch(arrow_arrays, arrow_schema)
        if self.columns_for_enrichment is not None:
            batch = batch.remove_columns(self.columns_to_remove)
        if self.columns_for_enrichment is not None:
            batch = batch.append_columns(self.columns_for_enrichment)

        batch = batch.append_columns(
            {"_extraction_timestamp": self.extraction_timestamp}
        )
        return batch

    def generate_batches(
        self, batch_size: int = 20_000, override_schema: pa.Schema | None = None
    ) -> Iterator[pa.RecordBatch]:
        self.arrow_schema = override_schema
        if self.engine is None:
            raise ValueError(
                "Cannot Generate batches with ArrowBatchReader without building an SQLAlchemy Engine"
            )
        if self.query is None:
            raise ValueError(
                "Cannot Generate batches with ArrowBatchReader without building an SQLAlchemy TextClause/Query"
            )

        with self.engine.connect().execution_options(stream_results=True) as connection:
            with connection.execute(self.query) as cursor_result:
                dialect = connection.engine.driver
                cursor_description = cursor_result.cursor.description
                if self.arrow_schema is None:
                    self.arrow_schema = create_arrow_schema(cursor_description, dialect)

                for source_batch in cursor_result.fetchmany(batch_size):
                    final_batch = self._compile_batch(source_batch, self.arrow_schema)
                    yield final_batch.collect()
                    time.sleep(0.001)


def create_batch_generator(
    name: str,
    engine: Engine,
    query: str | TextClause,
    bind_params: dict[str, Any] | None = None,
    batch_size: int = 20_000,
    override_schema: pa.Schema | None = None,
    remove_columns: str | list[str] | None = None,
    enrichment_map: dict[str, Any] | None = None,
) -> Iterator[pa.RecordBatch]:
    """
    A function that builds an ArrowBatchReader and yields from it the resulting Arrow RecordBatches

    Args:
        name (str): The name of the dataset for collection
        engine (Engine): An SQLAlchemy Engine of the source system of
            the data
        query (str|TextClause): The query with which the data are generated
            by the source system.
            It will be transformed internally to an SQLAlchemy TextClause if
            it is valid SQL statement.
            Can be a str or an SQLAlchemy TextClause.
        bind_params (Optional[dict[str, Any]]): The parameters with which the
            TextClause. Default is None.
        batch_size (int): The number of rows to collect on each iteration.
            Default is 20_000.
        override_schema (pyarrow.Schema): Manual Arrow Schema provision.
            It bypasses the automatic arrow translation.
            Default is None.
        remove_columns (str|list[str]|None): Columns to remove from the resulting
            Arrow RecordBatch. Default is None.
        enrichment_map (dict[str, Any]|None): Add columns and values to them to
            the resulting Arrow RecordBatch.Suggested to use only
            for metadata enrichment.

    Yields:
        Iterator[RecordBatch]: The translated from SQLAlchemy to Arrow RecordBatch.
    """
    batch_generator = ArrowBatchReader(name=name)
    (
        batch_generator.with_engine(engine)
        .with_query(query, bind_params)
        .add_columns_for_enrichment(enrichment_map)
        .add_columns_for_removal(remove_columns)
    )
    yield from batch_generator.generate_batches(batch_size, override_schema)
