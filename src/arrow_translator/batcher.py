import datetime
from collections.abc import Iterator, Sequence
from typing import Any, Optional, Self, final

import pyarrow as pa
from sqlalchemy import Engine, Row, TextClause, text

from .descriptor import create_arrow_schema
from .logger import LOGGER


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
            array_of_value = pa.repeat(value, self.num_of_rows)
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

    def __init__(
        self,
        name: str,
        engine: Engine,
        query: str | TextClause,
        bind_params: Optional[dict[str, Any]] = None,
        cols_to_remove: Optional[str | list[str]] = None,
        metadata_to_add: Optional[dict[str, Any]] = None,
    ):
        self.name = name
        self.extraction_timestamp = datetime.datetime.now().astimezone()
        LOGGER.debug(
            "Initialized Arrow Batch Generator for resource %s at %s",
            self.name,
            str(self.extraction_timestamp),
        )
        self.engine = engine
        self.query = query
        self.bind_params = bind_params
        self.text_clause = self._create_text_clause(query, bind_params)

    def _create_text_clause(
        self,
        q: str | TextClause,
        bind_params: dict[str, Any] | None,
    ):
        result = None
        if not isinstance(q, TextClause):
            try:
                LOGGER.debug(
                    "Transforming Raw Query to SQLAlchemy TextClause for resource %s",
                    self.name,
                )
                temp = text(q)
            except Exception:
                LOGGER.error(
                    "TextClause creation for resource %s failed. Exiting the program",
                    self.name,
                    exc_info=True,
                    stack_info=True,
                )
                exit()
            else:
                LOGGER.info(
                    "TextClause creation for resource %s successful",
                    self.name,
                    exc_info=True,
                )
                result = temp
        else:
            LOGGER.info(f"Query for resource {self.name} is already TextClause")
            result = q
        if bind_params is not None:
            if isinstance(bind_params, dict):
                try:
                    LOGGER.debug(
                        f"Binding Parameters to TextClause for resource {self.name}"
                    )
                    temp = result.bindparams(**bind_params)
                except Exception as error:
                    LOGGER.error(
                        f"Error when binding Parameters to TextClause for resource {self.name}"
                    )
                else:
                    result = temp
                    return result
        else:
            return result

    def _cursor_result_transposition(self, rows: Sequence[Row[Any]]):
        try:
            LOGGER.debug("Transposing source cursor result for resource %s", self.name)
            result = zip(*rows)
        except Exception:
            LOGGER.error(
                "Error in the transposition of the dataset %s. Exiting the program",
                self.name,
                exc_info=True,
                stack_info=True,
            )
            exit()
        else:
            LOGGER.info(
                "Created a zipped list of rows for resource %s",
                self.name,
            )
            return result

    def _transposed_arrow_arrays(
        self,
        transposed_data: zip,
    ) -> list[pa.Array]:
        try:
            LOGGER.debug(
                "Creating an Arrow Array for each transposed Cursor Result Column for resource %s",
                self.name,
            )
            result_arrow_arrays = [pa.array(col) for col in transposed_data]
        except Exception:
            LOGGER.error(
                "Error in the transformation of the zipped list to Arrow array for resource %s",
                self.name,
                stack_info=True,
                exc_info=True,
            )
            exit()
        else:
            LOGGER.info(f"Succesfully Created Arrow Arrays for resource {self.name}.")
            return result_arrow_arrays

    def _arrow_arrays_to_batch(self, arrow_arrays: list[pa.Array], schema: pa.Schema):
        try:
            LOGGER.debug("Creating Arrow RecordBatch for resource %s", self.name)
            result_record_batch = pa.record_batch(data=arrow_arrays, schema=schema)
        except Exception:
            LOGGER.error(
                "Error in the transformation of the arrow arrays to a RecordBatch for resource %s",
                self.name,
                exc_info=True,
                stack_info=True,
            )
            exit()
        else:
            result = Batch(result_record_batch)
            LOGGER.info("Created Arrow RecordBatch for resource %s", self.name)
            return result

    def _create_map_of_columns_for_enrichment(
        self, enrichment_map: dict[str, Any] | None = None
    ):
        if enrichment_map is None:
            return {"_extraction_timestamp": self.extraction_timestamp}
        else:
            temp = {
                **enrichment_map,
                "_extraction_timestamp": self.extraction_timestamp,
            }
            return temp

    def _compile_batch(
        self,
        cursor_result: Sequence[Row[Any]],
        arrow_schema: pa.Schema,
        columns_to_remove: list[str] | str | None = None,
        columns_for_enrichment: dict[str, Any] | None = None,
    ):
        if isinstance(columns_to_remove, str):
            columns_to_remove = [columns_to_remove]
        transposed = self._cursor_result_transposition(cursor_result)
        arrow_arrays = self._transposed_arrow_arrays(transposed)
        batch = self._arrow_arrays_to_batch(arrow_arrays, arrow_schema)
        if columns_to_remove is not None:
            LOGGER.info(
                "Removing columns (%s) from dataset with  for resource %s",
                str(columns_to_remove),
                self.name,
            )
            batch = batch.remove_columns(columns_to_remove)
        final_map = self._create_map_of_columns_for_enrichment(columns_for_enrichment)
        batch = batch.append_columns(final_map)
        LOGGER.info("Appending enrichment for resource %s", self.name)
        return batch

    def generate_batches(
        self,
        batch_size: int = 20_000,
        override_schema: pa.Schema | None = None,
        columns_to_remove: list[str] | str | None = None,
        enrichment_map: dict[str, Any] | None = None,
    ) -> Iterator[pa.RecordBatch]:
        arrow_schema = override_schema
        if self.engine is None:
            raise ValueError(
                "Cannot Generate batches with ArrowBatchReader without building an SQLAlchemy Engine"
            )
        if self.query is None:
            raise ValueError(
                "Cannot Generate batches with ArrowBatchReader without building an SQLAlchemy TextClause/Query"
            )

        with self.engine.connect().execution_options(stream_results=True) as connection:
            with connection.execute(statement=self.text_clause) as cursor_result:
                driver = connection.engine.driver
                cursor_description = cursor_result.cursor.description
                if arrow_schema is None:
                    LOGGER.warning(
                        "Arrow Schema wan not provided for resource %s. It will be created ...",
                        self.name,
                        stack_info=True,
                    )
                    arrow_schema = create_arrow_schema(cursor_description, driver)
                while source_batch := cursor_result.fetchmany(batch_size):
                    final_batch = self._compile_batch(
                        source_batch, arrow_schema, columns_to_remove, enrichment_map
                    )
                    yield final_batch.collect()
                    LOGGER.info(
                        "Generated Arrow RecordBatch for resource %s || Size: %s||Rows: %s",
                        self.name,
                        final_batch.size,
                        final_batch.num_of_rows,
                    )

    # def create_batch_reader(
    #     self, batch_size: int = 20_000, override_schema: Optional[pa.Schema] = None
    # ):
    #     arrow_schema = override_schema
    #     batch_generator = self.generate_batches(
    #         batch_size=batch_size, override_schema=arrow_schema
    #     )
    #     if arrow_schema is None:
    #         first_batch = next(batch_generator)
    #         arrow_schema = first_batch.schema

    #         def recompiled():
    #             yield first_batch
    #             yield from batch_generator

    #         batch_generator = recompiled()
    #     return pa.RecordBatchReader.from_batches(arrow_schema, batch_generator)


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
    batch_generator = ArrowBatchReader(
        name=name,
        engine=engine,
        query=query,
        bind_params=bind_params,
        cols_to_remove=remove_columns,
        metadata_to_add=enrichment_map,
    )
    yield from batch_generator.generate_batches(batch_size, override_schema)


# def create_record_batch_reader(
#     name: str,
#     engine: Engine,
#     query: str | TextClause,
#     bind_params: dict[str, Any] | None = None,
#     batch_size: int = 20_000,
#     override_schema: pa.Schema | None = None,
#     remove_columns: str | list[str] | None = None,
#     enrichment_map: dict[str, Any] | None = None,
# ) -> pa.RecordBatchReader:
#     """
#     A function that builds an ArrowBatchReader and yields from it the resulting Arrow RecordBatches

#     Args:
#         name (str): The name of the dataset for collection
#         engine (Engine): An SQLAlchemy Engine of the source system of
#             the data
#         query (str|TextClause): The query with which the data are generated
#             by the source system.
#             It will be transformed internally to an SQLAlchemy TextClause if
#             it is valid SQL statement.
#             Can be a str or an SQLAlchemy TextClause.
#         bind_params (Optional[dict[str, Any]]): The parameters with which the
#             TextClause. Default is None.
#         batch_size (int): The number of rows to collect on each iteration.
#             Default is 20_000.
#         override_schema (pyarrow.Schema): Manual Arrow Schema provision.
#             It bypasses the automatic arrow translation.
#             Default is None.
#         remove_columns (str|list[str]|None): Columns to remove from the resulting
#             Arrow RecordBatch. Default is None.
#         enrichment_map (dict[str, Any]|None): Add columns and values to them to
#             the resulting Arrow RecordBatch.Suggested to use only
#             for metadata enrichment.

#     Yields:
#         RecordBatchReader: Create a RecordBatchReader from an SQLAlchemy Engine.
#     """
#     batch_generator = ArrowBatchReader(
#         name=name,
#         engine=engine,
#         query=query,
#         bind_params=bind_params,
#         cols_to_remove=remove_columns,
#         metadata_to_add=enrichment_map,
#     )
#     return batch_generator.create_batch_reader(
#         batch_size=batch_size, override_schema=override_schema
#     )


# ===================================== FOR TESTING ====================================
def transpose_cursor_result(name: str, cursor_result: Sequence[Row[Any]]):
    print(cursor_result)
    try:
        result = zip(*cursor_result)
    except Exception as error:
        print(f"Error in the transposition of the dataset {name}")
        print(error)
        raise error
    else:
        return result

def zipped_result(name: str, cursor_result: Sequence[Row[Any]]):
    zipped = zip(*cursor_result)
    arrays = [pa.array(col) for col in zipped]
    return arrays


def create_arrow_arrays(
    name: str,
    transposed_data: zip,
):
    try:
        result_arrow_arrays = [pa.array(col) for col in transposed_data]
    except Exception as error:
        print(
            f"Error in the transformation of the numpy array to Arrow array for resource {name}"
        )
        print(error)
    else:
        return result_arrow_arrays


def create_arrow_batch_from_arrow_arrays(
    name: str, arrow_arrays: list[pa.Array], schema: pa.Schema
):
    try:
        result_record_batch = pa.record_batch(data=arrow_arrays, schema=schema)
    except Exception as error:
        print(
            f"Error in the transformation of the arrow arrays to a RecordBatch for resource {name}"
        )
        print(error)
    else:
        return Batch(result_record_batch)
