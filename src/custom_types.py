from typing import Dict, Iterator

import enum

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

custom_any = str | int
ModuleRecord = Dict[custom_any, Iterator[IOOP]]
TypeRecord = Dict[IOType, Iterator[IOOP]]
