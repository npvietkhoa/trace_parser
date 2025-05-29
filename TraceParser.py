import os
import darshan
from typing import Iterator
from itertools import chain

from custom_types import IOModule, IOType, ModuleRecord, TypeRecord
from IOOP import IOOP


def ops_generator(
    records, rank, *, mod_name: IOModule, io_type: IOType = IOType.ALL
) -> Iterator:
    mod_record = records[mod_name.value]
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


def records_parse(records, mod: IOModule) -> ModuleRecord:
    # Parse record of given module to dict of [rank - IOOPs]
    mod_records = records[mod.value]
    ranks = [log["rank"] for log in mod_records]

    record_dict = dict()
    for rank in ranks:
        record_dict[rank] = ops_generator(records, rank, mod_name=mod)

    return record_dict


def trace_parse(fp: str) -> dict[IOModule, ModuleRecord]:
    assert os.path.isfile(fp)
    records = darshan.DarshanReport(fp, read_all=True, dtype="dict").records

    record_dict = dict()
    for mod in IOModule:
        record_dict[mod] = records_parse(records, mod)

    return record_dict


def io_type_report_parser(fp: str) -> TypeRecord:
    assert os.path.isfile(fp)
    mod_records = trace_parse(fp)

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
