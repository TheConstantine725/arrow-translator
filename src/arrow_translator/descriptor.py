from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from .typemaps import Lexicon

type CursorDescription = Sequence[tuple[Any]]


@dataclass
class FieldDescriptor:
    name: str
    type_code: Any | None
    display_size: Any | None
    internal_size: Any | None
    precision: int | None
    scale: int | None
    null_ok: bool | None

    @property
    def is_nullable(self) -> bool:
        if self.null_ok or self.null_ok is None:
            return True
        return False

    @property
    def fixed_name(self) -> str:
        _name = self.name
        for pat in ("//", "\\", "/"):
            if pat in _name:
                _name = _name.replace(pat, "_")
        return _name.lower()

    def to_pyarrow(self, dialect: str) -> pa.Field:
        data_type = Lexicon.to_pyarrow(dialect, self.type_code)
        if isinstance(data_type, pa.Decimal128Type):
            data_type = pa.decimal128(self.precision, self.scale)
        elif isinstance(data_type, pa.Decimal256Type):
            data_type = pa.decimal256(self.precision, self.scale)
        return pa.field(name=self.fixed_name, type=data_type, nullable=self.is_nullable)


class TableDescriptor:
    def __init__(self, cursor_description: CursorDescription):
        self.cursor_description: CursorDescription = cursor_description

    @property
    def field_descriptors(self) -> list[FieldDescriptor]:
        return [FieldDescriptor(*field) for field in self.cursor_description]

    def to_arrow_schema(self, dialect: str) -> pa.Schema:
        return pa.schema(
            [_field.to_pyarrow(dialect) for _field in self.field_descriptors]
        )

    def __iter__(self):
        yield from self.field_descriptors


def create_arrow_schema(
    cursor_description: CursorDescription, dialect: str
) -> pa.Schema:
    return TableDescriptor(cursor_description).to_arrow_schema(dialect)


def create_table_descriptor(cursor_description: CursorDescription):
    return TableDescriptor(cursor_description)
