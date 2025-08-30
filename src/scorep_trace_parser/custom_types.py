import enum
from typing import TypeAlias
import otf2

class IOMod(enum.Enum):
    WRITE = otf2.IoOperationMode.WRITE
    READ = otf2.IoOperationMode.READ

    @classmethod
    def from_otf2(cls, op_mode):
        for member in cls:
            if member.value == op_mode:
                return member
        raise ValueError(f"No matching IOMod for {op_mode}")

class IOParadigm(enum.Enum):
    # https://scorepci.pages.jsc.fz-juelich.de/otf2-pipelines/doc.r4703/group__io.html
    POSIX = "POSIX"
    MPIIO = "MPI-IO"
    ISOC = "ISOC"

    @classmethod
    def from_otf2(cls, io_handle: otf2.definitions.IoHandle):
        for member in cls:
            if member.value == io_handle.io_paradigm.identification:
                return member
        raise ValueError(f"No matching IOPradigm for {io_handle.io_paradigm}")

custom_any: TypeAlias = str | int