import datetime
from collections.abc import Iterator, Sequence
from time import sleep
from typing import Any, Optional

import pyarrow as pa
from sqlalchemy import Engine, Row, TextClause, text

from .descriptor import create_arrow_schema
from .logger import get_logger


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
        batch_size: int = 20_000,
        cols_to_remove: Optional[str | list[str]] = None,
        metadata_to_add: Optional[dict[str, Any]] = None,
    ):
        self.name = name
        self.extraction_timestamp = datetime.datetime.now().astimezone()
        get_logger().debug(
            "Initialized Arrow Batch Generator for resource %s at %s",
            self.name,
            str(self.extraction_timestamp),
            extra={"resource_name": self.name},
        )
        self.engine = engine
        self.query = query
        self.bind_params = bind_params
        self.text_clause = self.__handle_text_clause(query, bind_params)
        self.batch_size = batch_size
        self.columns_to_remove = self.__handle_columns_to_remove(cols_to_remove)
        self.metadata_enrichment = self.__handle_metadata_enrichment(metadata_to_add)

    def __handle_columns_to_remove(self, _cols_to_remove: Optional[str | list] = None):
        if _cols_to_remove is None:
            return set()
        if isinstance(_cols_to_remove, str):
            return {_cols_to_remove}
        elif isinstance(_cols_to_remove, list):
            return set(_cols_to_remove)

    def __handle_metadata_enrichment(self, _metadata_map: Optional[dict[str, Any]]):
        _temp = {}
        result = {}
        if _metadata_map is None:
            _temp = {"_extraction_timestamp": self.extraction_timestamp}
        else:
            _temp = {
                **_metadata_map,
                "_extraction_timestamp": self.extraction_timestamp,
            }

        for name, value in _temp.items():
            __field = pa.field(name=name, type=pa.scalar(value).type)
            result[__field] = value
        return result

    def __handle_text_clause(
        self,
        q: str | TextClause,
        bind_params: dict[str, Any] | None,
    ):
        result = None
        if not isinstance(q, TextClause):
            try:
                get_logger().debug(
                    "Transforming Raw Query to SQLAlchemy TextClause for resource %s",
                    self.name,
                    extra={"resource_name": self.name},
                )
                temp = text(q)
            except Exception:
                get_logger().error(
                    "TextClause creation for resource %s failed. Exiting the program",
                    self.name,
                    exc_info=True,
                    stack_info=True,
                    extra={"resource_name": self.name},
                )
                raise
            else:
                get_logger().info(
                    "TextClause creation for resource %s successful",
                    self.name,
                    extra={"resource_name": self.name},
                )
                result = temp
        else:
            get_logger().info(
                "Query for resource %s is already TextClause",
                self.name,
                extra={"resource_name": self.name},
            )
            result = q
        if bind_params is not None:
            if isinstance(bind_params, dict):
                try:
                    get_logger().debug(
                        "Binding Parameters to TextClause for resource %s",
                        self.name,
                        extra={"resource_name": self.name},
                    )
                    temp = result.bindparams(**bind_params)
                except Exception as error:
                    get_logger().error(
                        "Error when binding Parameters to TextClause for resource %s",
                        self.name,
                        exc_info=True,
                        extra={"resource_name": self.name},
                    )
                    raise error
                else:
                    result = temp
                    return result
        else:
            return result

    def _translate_original_schema(self, cursor_description, driver):
        get_logger().debug(
            "Translating original schema for resource %s",
            self.name,
            extra={"resource_name": self.name},
        )
        res = create_arrow_schema(cursor_description, driver)
        return res

    def _precompute_final_schema(self, original_schema: pa.Schema):
        try:
            get_logger().debug(
                "Rebuilding final arrow schema for resource %s",
                self.name,
                extra={"resource_name": self.name},
            )
            temp = self._kept_fields(original_schema)
            temp.extend(self.metadata_enrichment.keys())
            result = pa.schema(temp)
        except Exception:
            get_logger().error(
                "Error in the creation of the final arrow schema for resource %s",
                self.name,
                extra={"resource_name": self.name},
            )
            raise
        else:
            return result

    def _kept_fields(self, schema: pa.Schema) -> list[pa.Field]:
        return [field for field in schema if field.name not in self.columns_to_remove]

    def _row_to_columns_transposition(self, rows: Sequence[Row[Any]]):
        try:
            get_logger().debug(
                "Transposing source cursor result for resource %s",
                self.name,
                extra={"resource_name": self.name},
            )
            result = zip(*rows)
        except Exception as error:
            get_logger().error(
                "Error in the transposition of the dataset %s. Exiting the program",
                self.name,
                exc_info=True,
                stack_info=True,
                extra={"resource_name": self.name},
            )
            raise error
        else:
            get_logger().info(
                "Created a zipped list of rows for resource %s",
                self.name,
                extra={"resource_name": self.name},
            )
            return result

    def _compile_arrays_to_record_batch(
        self, transposed_columns: zip, schema: pa.Schema, kept_columns: set[str]
    ):
        temp_arrays = []
        temp_schema = []
        try:
            get_logger().debug(
                "Building Arrays for the Arrow RecordBatch for resource %s",
                self.name,
                extra={"resource_name": self.name},
            )
            for array, field in zip(transposed_columns, schema):
                if field.name in kept_columns:
                    try:
                        get_logger().debug(
                            "Creating array with then name %s of datatype %s for resource %s",
                            field.name,
                            field.type,
                            self.name,
                        )
                        _loop_array = pa.array(obj=array, type=field.type)
                    except Exception:
                        get_logger().error(
                            "Error in the creation of the array with then name %s of datatype %s for resource %s. Got array of type %s",
                            field.name,
                            field.type,
                            self.name,
                            str(type(array)),
                        )
                        raise
                    else:
                        temp_arrays.append(_loop_array)
                        temp_schema.append(field)
                else:
                    get_logger().debug(
                        "Ignored Field %s for resource %s. Removing from in process schema...",
                        field.name,
                        self.name,
                        extra={"resource_name": self.name},
                    )

            num_of_rows = len(temp_arrays[0]) if temp_arrays else 0
            for field, value in self.metadata_enrichment.items():
                get_logger().debug(
                    "Enriching with metadata with the name %s the resource %s",
                    field.name,
                    self.name,
                    extra={"resource_name": self.name},
                )
                temp_arrays.append(
                    pa.repeat(value=value, size=num_of_rows).cast(field.type)
                )
                temp_schema.append(field)

            record_batch = pa.record_batch(
                data=temp_arrays,
                schema=pa.schema(temp_schema),
            )
        except Exception as error:
            get_logger().error(
                "Error in the transformation of the arrow arrays to a RecordBatch for resource %s",
                self.name,
                exc_info=True,
                stack_info=True,
                extra={"resource_name": self.name},
            )
            raise error
        else:
            get_logger().info(
                "Created Arrow RecordBatch for resource %s",
                self.name,
                extra={"resource_name": self.name},
            )
            return record_batch

    def generate_batches(
        self,
        override_schema: pa.Schema | None = None,
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
                    get_logger().warning(
                        "Arrow Schema wan not provided for resource %s. It will be created ...",
                        self.name,
                        extra={"resource_name": self.name},
                    )
                    arrow_schema = self._translate_original_schema(
                        cursor_description, driver
                    )
                kept_fields = set(
                    field.name for field in self._kept_fields(arrow_schema)
                )
                batch_number = 0
                total_rows = 0
                while source_batch := cursor_result.fetchmany(self.batch_size):
                    final_batch = self._compile_arrays_to_record_batch(
                        self._row_to_columns_transposition(source_batch),
                        arrow_schema,
                        kept_fields,
                    )
                    total_rows += final_batch.num_rows
                    yield final_batch
                    get_logger().info(
                        "Generated Arrow RecordBatch for resource %s",
                        self.name,
                        extra={
                            "resource_name": self.name,
                            "batch_id": batch_number,
                            "number_of_rows": final_batch.num_rows,
                            "total_rows": total_rows,
                            "batch_size": final_batch.nbytes,
                        },
                    )
                    batch_number += 1
                    sleep(0.01)

    def create_batch_reader(self, override_schema: pa.Schema):
        arrow_schema = override_schema
        if self.engine is None:
            raise ValueError(
                "Cannot Generate batches with ArrowBatchReader without building an SQLAlchemy Engine"
            )
        if self.query is None:
            raise ValueError(
                "Cannot Generate batches with ArrowBatchReader without building an SQLAlchemy TextClause/Query"
            )
        connection = self.engine.connect().execution_options(stream_results=True)
        query = self.text_clause
        cursor_result = connection.execute(query)
        try:
            if arrow_schema is None:
                driver = connection.engine.driver
                cursor_description = cursor_result.cursor.description
                get_logger().warning(
                    "Arrow Schema was not provided for resource %s. It will be created ...",
                    self.name,
                    extra={"resource_name": self.name},
                )
                arrow_schema = self._translate_original_schema(
                    cursor_description, driver
                )
            final_arrow_schema = self._precompute_final_schema(arrow_schema)

        except Exception as error:
            cursor_result.close()
            connection.close()
            raise error

        def _batcher() -> Iterator[pa.RecordBatch]:
            try:
                kept_fields = set(
                    field.name for field in self._kept_fields(arrow_schema)
                )
                total_rows = 0
                batch_number = 0
                while source_batch := cursor_result.fetchmany(self.batch_size):
                    final_batch = self._compile_arrays_to_record_batch(
                        self._row_to_columns_transposition(source_batch),
                        arrow_schema,
                        kept_fields,
                    )
                    total_rows += final_batch.num_rows
                    yield final_batch
                    get_logger().info(
                        "Generated Arrow RecordBatch for resource %s",
                        self.name,
                        extra={
                            "resource_name": self.name,
                            "batch_id": batch_number,
                            "number_of_rows": final_batch.num_rows,
                            "total_rows": total_rows,
                            "batch_size": final_batch.nbytes,
                        },
                    )
                    batch_number += 1
                    sleep(0.01)
            finally:
                cursor_result.close()
                connection.close()

        return pa.RecordBatchReader.from_batches(final_arrow_schema, _batcher())


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
        batch_size=batch_size,
        cols_to_remove=remove_columns,
        metadata_to_add=enrichment_map,
    )
    yield from batch_generator.generate_batches(override_schema)


def create_record_batch_reader(
    name: str,
    engine: Engine,
    query: str | TextClause,
    bind_params: dict[str, Any] | None = None,
    batch_size: int = 20_000,
    override_schema: pa.Schema | None = None,
    remove_columns: str | list[str] | None = None,
    enrichment_map: dict[str, Any] | None = None,
) -> pa.RecordBatchReader:
    """
    A function that builds an RecordBatchReader from SQLAlchemy Engine and TextClause

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
        RecordBatchReader: The translated from SQLAlchemy to Arrow RecordBatch.
    """
    batch_generator = ArrowBatchReader(
        name=name,
        engine=engine,
        query=query,
        bind_params=bind_params,
        batch_size=batch_size,
        cols_to_remove=remove_columns,
        metadata_to_add=enrichment_map,
    )
    return batch_generator.create_batch_reader(override_schema)
