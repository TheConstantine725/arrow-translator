from .typemaps import LEXICON
from typing import Any
import pyarrow as pa
from dataclasses import dataclass

@dataclass
class FieldDescriptor:
    name: str
    type_code: Any|None
    internal_size: Any|None
    precision: int|None
    scale: int|None
    null_ok: bool|None

    @property
    def is_nullable(self):
        if self.null_ok == True or self.null_ok is None:
            return True
        return False
