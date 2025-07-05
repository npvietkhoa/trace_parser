from enum import Enum

class IOMod(Enum):
    READ = "READ"
    WRITE = "WRITE"
    MISC = "MISC"

class IOPradigm(Enum):
    POSIX = "POSIX"
    HDF5 = "HDF5"
    MPIIO = "MPIIO"


_GROUPED_FUNCTIONS = {
    # POSIX
    (IOMod.READ, IOPradigm.POSIX): [
        "open", "open64", "fopen", "fopen64", "fdopen",
        "read", "pread", "pread64", "readv", "fread",
    ],
    (IOMod.WRITE, IOPradigm.POSIX): [
        "creat", "creat64", "write", "pwrite", "pwrite64", "writev", "fwrite",
    ],
    (IOMod.MISC, IOPradigm.POSIX): [
        "close", "fclose", "lseek", "lseek64", "fseek", "fseeko",
        "dup", "dup2", "fsync", "fdatasync", "stat", "fstat", "lstat",
        "access", "chmod", "chown", "unlink", "remove", "rename", "umask", "__lxstat",
        "getcwd", "fcntl"
    ],

    # MPI-IO
    (IOMod.READ, IOPradigm.MPIIO): [
        "MPI_File_open",
        "MPI_File_read", "MPI_File_read_all", "MPI_File_read_ordered",
        "MPI_File_read_at", "MPI_File_read_at_all", "MPI_File_read_shared",
        "MPI_File_iread", "MPI_File_iread_at", "MPI_File_iread_shared",
    ],
    (IOMod.WRITE, IOPradigm.MPIIO): [
        "MPI_File_write", "MPI_File_write_all", "MPI_File_write_ordered",
        "MPI_File_write_at", "MPI_File_write_at_all", "MPI_File_write_shared",
        "MPI_File_iwrite", "MPI_File_iwrite_at", "MPI_File_iwrite_shared",
        "MPI_File_write_at_all",
    ],
    (IOMod.MISC, IOPradigm.MPIIO): [
        "MPI_File_close", "MPI_File_seek", "MPI_File_set_view",
        "MPI_File_get_size", "MPI_File_preallocate",
        "MPI_File_get_position", "MPI_File_get_byte_offset",
        "MPI_Comm_size", "MPI_Comm_rank", "MPI_File_set_view", "MPI_Send",
        "MPI_Recv", "MPI_Bcast", "MPI_Barrier",
        "MPI_Gather", "MPI_Scatter", "MPI_Reduce", "MPI_Waitall", "MPI_Wait",
        "MPI_Comm_dup", "MPI_Type_commit", "MPI_Irecv", "MPI_Isend", "MPI_Allreduce"
    ],

    # HDF5
    (IOMod.READ, IOPradigm.HDF5): [
        "H5Dread", "H5Fopen", "H5Gopen", "H5Aread",
    ],
    (IOMod.WRITE, IOPradigm.HDF5): [
        "H5Dwrite", "H5Fcreate", "H5Gcreate", "H5Awrite",
    ],
    (IOMod.MISC, IOPradigm.HDF5): [
        "H5Fclose", "H5Dclose", "H5Gclose", "H5Aclose",
        "H5Screate", "H5Sclose", "H5Tcopy",
        "H5Pcreate", "H5Pset_fapl_mpio", "H5Pset_dxpl_mpio",
    ],
}

_FUNCTION_CLASS_MAP = {
    fn: (iomod, paradigm)
    for (iomod, paradigm), fns in _GROUPED_FUNCTIONS.items()
    for fn in fns
}

def map_io_fn(fn: str) -> tuple[IOMod, IOPradigm]:
    """
    Maps a function name to its corresponding IOMod and IOPradigm.
    
    :param fn: The function name to map.
    :type fn: str
    :return: A tuple of (IOMod, IOPradigm) corresponding to the function name.
    :rtype: tuple[IOMod, IOPradigm]
    :raises ValueError: If the function name is not recognized.
    """
    try:
        return _FUNCTION_CLASS_MAP[fn]
    except KeyError:
        raise ValueError(f"Unknown function: {fn}")