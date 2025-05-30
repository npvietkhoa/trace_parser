import os
import darshan
from typing import Iterator
from itertools import chain

from custom_types import IOModule, IOType, ModuleRecord, TypeRecord
from IOOP import IOOP


class TraceParser:
    """
    A class to parse Darshan trace files and extract IO operations.
    """

    def __init__(self, fp: str):
        assert os.path.isfile(fp), f"File not found: {fp}"
        assert fp.endswith(".darshan"), f"Invalid file type: {fp}"
        self.fp = fp
        self.records = (darshan
                        .DarshanReport(self.fp, read_all=True, dtype="dict")
                        .records)

    def parse_trace(self) -> dict[IOModule, ModuleRecord]:
        """
        Parse and process trace data into a dictionary containing records for each
        I/O module. The method reads all records from the DarshanReport and organizes
        them by the specific I/O module type into the resulting dictionary.

        :return: A dictionary mapping each I/O module to its associated record.
        :rtype: dict[IOModule, ModuleRecord]
        """

        record_dict = dict()
        for mod in IOModule:
            record_dict[mod] = self.parse_records(self.records[mod])

        return record_dict

    def parse_records(self, mod: IOModule) -> ModuleRecord:
        """
        Parses module records and generates a record dictionary where each rank is
        mapped to its corresponding operational logs.

        :param mod: Module instance which contains a key value used for fetching
                    related logs from the `records` attribute.
        :type mod: IOModule
        :return: A dictionary mapping each rank from the logs of the given module
                 to the generated operations for that rank.
        :rtype: ModuleRecord
        """
        mod_record = self.records[mod.value]
        ranks = [log["rank"] for log in mod_record]

        record_dict = dict()
        for rank in ranks:
            record_dict[rank] = self._ops_generator(rank, mod_name=mod)
        return record_dict

    def parse_io_type_record(self) -> TypeRecord:
        """
        Parses IO type records by iterating through trace data, organizing them
        into a categorized structure, and returning the collected type-based
        operation records.

        This method processes a series of modular trace records, combines
        inner operations through iteration, and then aggregates the operations
        grouped by their respective types in a dictionary.

        :return: A dictionary where keys represent operation types and values
            are lists of corresponding operations.
        :rtype: dict
        """
        mod_records = self.parse_trace()

        ioop_iter = iter([])
        for _, mod_record in mod_records.items():
            for _, rank_ioop_iter in mod_record.items():
                ioop_iter = chain(ioop_iter, rank_ioop_iter)

        type_ops_collection = dict()
        while True:
            try:
                op = next(ioop_iter)
                collection = type_ops_collection.setdefault(op.type, [])
                collection.append(op)
            except StopIteration:
                break

        return type_ops_collection

    def _ops_generator(
        self, rank, *, mod_name: IOModule, io_type: IOType = IOType.ALL
    ) -> Iterator:
        """
        Generates an iterator of IO operations for a specified rank, module, and IO type. The
        function filters records for a given rank within a specific module and retrieves
        operations based on the requested IO type(s). If the IO type is set to IOType.ALL,
        both READ and WRITE operations are considered. For each matching operation
        segment in the filtered records, an IOOP object is yielded with corresponding details.

        :param rank: The rank for which the IO operations need to be retrieved.
        :type rank: int
        :param mod_name: The name of the module associated with the operations.
        :type mod_name: IOModule
        :param io_type: The type of IO operations to retrieve. Defaults to IOType.ALL.
        :type io_type: IOType
        :return: An iterator yielding IOOP objects representing the IO operations.
        :rtype: Iterator[IOOP]
        """
        mod_record = self.records[mod_name.value]
        rank_log = list(filter(lambda record: record["rank"] == rank, mod_record))

        assert len(rank_log) == 1, f"Found > 1 or no record for Rank: {rank_log}"
        rank_log = rank_log[0]

        io_types = [io_type] if io_type != IOType.ALL else [IOType.READ, IOType.WRITE]

        for current_type in io_types:
            segment_key = current_type.get_seg_key()
            for op_seg in rank_log[segment_key]:
                yield IOOP(
                    mod=mod_name,
                    type=current_type,
                    rank=rank,
                    start_time=op_seg["start_time"],
                    end_time=op_seg["end_time"],
                    offset=op_seg["offset"],
                    length=op_seg["length"],
                )