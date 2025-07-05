
import os
from functools import cache, reduce
from itertools import chain
from typing import Any, Callable, Dict, Iterator, Tuple
from recorder_viz import RecorderReader
from .custom_types import IOMod, IOPradigm
from .IOOP import IOOP

class TraceParser:
    """
    A class to parse and handle IO operations from a trace file.
    """

    def __init__(self, rp: str):
        """
            Initializes the TraceParser with a trace file.

            :param rp: Path to the trace folder.
            """
        assert os.path.isdir(rp), f"Recorder trace folder not found: {rp}"
        self.rp = rp
        self.rr = RecorderReader(self.rp)

    @cache
    def parse_trace(self) -> dict[int, Iterator[IOOP]]|Any:
        """
        Parses the trace file and returns a dictionary of IO operations for each rank.
        :return: A dictionary where keys are ranks and values are iterators of IOOP objects
        representing IO operations.
        """
        funcs = self.rr.funcs
        records_by_rank = self.rr.records
        total_ranks = self.rr.GM.total_ranks
        local_meta = self.rr.LMs

        def make_ioop(record):
            return IOOP(
                fname=funcs[record.func_id],
                fid=record.func_id,
                start_time=record.tstart,
                end_time=record.tend,
                call_depth=record.call_depth,
                rargs=record.args
            )

        return {
            rank: (make_ioop(record) for record in records_by_rank[rank][:local_meta[rank].total_records])
            for rank in range(total_ranks)
        }
  


    def aggregate_op_stat(
        self,
        stat_fn: Callable[[IOOP], Any]
    ) -> Any:
        """
        Traverse all IOOP instances from parse_trace, apply stat_fn to each,
        and collect results in a dict keyed by (mod, paradigm).
        
        Args:
            stat_fn: Function to apply to each IOOP instance
            
        Returns:
            Dictionary with (IOMod, IOPradigm) keys and list of stat_fn results as values
        """
        # Flatten all IOOP lists from the location dictionary
        ops_stream = chain.from_iterable(self.parse_trace().values())

        # Map each IOOP to ((mod, paradigm), stat_fn(op))
        kv_stream = map(lambda op: ((op.mod, op.paradigm), stat_fn(op)), ops_stream)

        # Reduce into grouped dict
        def reducer(
                acc: Dict[Tuple[IOPradigm, IOMod], list[Any]],
                kv: Tuple[Tuple[IOPradigm, IOMod], Any]
        ) -> Dict[Tuple[IOPradigm, IOMod], list[Any]]:
            key, value = kv
            if key not in acc:
                acc[key] = []
            acc[key].append(value)
            return acc

        return reduce(reducer, kv_stream, {})
