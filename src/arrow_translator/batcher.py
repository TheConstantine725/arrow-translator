import time
from typing import Self, Any, final
from collections.abc import Sequence, Generator, Iterator
from sqlalchemy import Engine, text, TextClause, Row
import datetime
import numpy as np

import pyarrow as pa
from .descriptor import create_arrow_schema


class Batch:
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
    def __init__(self, name: str):
        self.name: str = name
        self.extraction_timestamp: datetime.datetime = (
            datetime.datetime.now().astimezone()
        )
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
        temp_array = np.array(rows, dtype=object)
        try:
            result = temp_array.transpose()
        except Exception as error:
            print(f"Error in the transposition of the dataset {self.name}")
            print(error)
        else:
            return result

    def _transposed_arrow_arrays(
        self,
        transposed_data: np.ndarray[Any],
    ):
        result_arrow_arrays: list[pa.Array[Any]] = []
        try:
            for column in transposed_data:
                result_arrow_arrays.append(pa.array(column))
        except Exception as error:
            print("Error in the transformation of the numpy array to Arrow array")
            print(error)
        else:
            return result_arrow_arrays

    def _arrow_arrays_to_batch(
        self, arrow_arrays: list[pa.Array[Any]], schema: pa.Schema
    ):
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
                dialect = connection.dialect.name
                cursor_description = cursor_result.cursor.description
                if self.arrow_schema is None:
                    self.arrow_schema = create_arrow_schema(cursor_description, dialect)

                for source_batch in cursor_result.fetchmany(batch_size):
                    final_batch = self._compile_batch(source_batch, self.arrow_schema)
                    yield final_batch.collect()
                    time.sleep(0.01)


def create_batch_generator(
    name: str,
    engine: Engine,
    query: str | TextClause,
    bind_params: dict[str, Any],
    batch_size: int = 20_000,
    override_schema: pa.Schema | None = None,
    remove_columns: str | list[str] | None = None,
    enrichment_map: dict[str, Any] | None = None,
) -> Iterator[pa.RecordBatch]:
    batch_generator = ArrowBatchReader(name=name)
    (
        batch_generator.with_engine(engine)
        .with_query(query, bind_params)
        .add_columns_for_enrichment(enrichment_map)
        .add_columns_for_removal(remove_columns)
    )
    yield from batch_generator.generate_batches(batch_size, override_schema)
