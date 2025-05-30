from __future__ import annotations
from typing import Dict, Iterator, TypeAlias, TYPE_CHECKING

import enum

if TYPE_CHECKING:
    from IOOP import IOOP

class IOModule(enum.Enum):
    DXT_MPIIO = "DXT_MPIIO"
    DXT_POSIX = "DXT_POSIX"


class IOType(enum.Enum):
    WRITE = "write"
    READ = "read"
    ALL = "all"

    def get_seg_key(self) -> str | None:
        if self.name != "ALL":
            return f"{self.value}_segments"
        return None

custom_any: TypeAlias = str | int
ModuleRecord: TypeAlias = Dict[custom_any, Iterator['IOOP']]
TypeRecord: TypeAlias = Dict[IOType, Iterator['IOOP']]
