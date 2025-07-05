import os 
import otf2
from .IOOP import IOOP
from .custom_types import IOMod, IOPradigm
from collections import defaultdict
from itertools import chain
from typing import Iterator, Callable, Dict, Tuple, Any
from functools import cache, reduce

class TraceParser:
    def __init__(self, fp: str):
        assert os.path.isfile(fp), f"File not found: {fp}"
        assert fp.endswith(".otf2"), f"Invalid file type: {fp}"
        self.fp = fp


    @property
    def time_resolution(self) -> float:
        """
        Get the time resolution of the trace file.
        
        Returns:
            Time resolution in seconds.
        """
        with otf2.reader.open(self.fp) as trace:
            return trace.timer_resolution

    @cache
    def parse_trace(self) -> dict[otf2.definitions.Location, IOOP]:
        event_stack = defaultdict(list)
        ioop_stack = defaultdict(list)
        with otf2.reader.open(self.fp) as trace:
            for loc, event in trace.events:
                if isinstance(event, otf2.events.IoOperationBegin):
                    event_stack[loc].append(event)
                elif isinstance(event, otf2.events.IoOperationComplete):
                    assert event_stack[loc], "IoOperationEnd without matching IoOperationBegin"
                    matching_events = list(filter(lambda e: e.matching_id == event.matching_id, event_stack[loc]))
                    assert len(matching_events) == 1, "Multiple IoOperationBegin events found for the same IoOperationEnd"
                    start_event = matching_events[0]
                    assert start_event.time < event.time, "IoOperationBegin time must be less than IoOperationEnd time"
                    assert start_event.handle == event.handle, "IoOperationBegin and IoOperationEnd must have the same handle"
                    assert start_event.bytes_request == event.bytes_result, "IoOperationBegin and IoOperationEnd must have the same byte request size"
                    event_stack[loc].remove(start_event)
                    ioop = IOOP(
                        mod=IOMod.from_otf2(start_event.mode),
                        paradigm=IOPradigm.from_otf2(start_event.handle),
                        start_time=start_event.time,
                        end_time=event.time,
                        bytes_request=start_event.bytes_request,
                        bytes_result=event.bytes_result
                    )
                    ioop_stack[loc].append(ioop)
        return ioop_stack


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
                acc: Dict[Tuple[IOMod, IOPradigm], list[Any]],
                kv: Tuple[Tuple[IOMod, IOPradigm], Any]
        ) -> Dict[Tuple[IOMod, IOPradigm], list[Any]]:
            key, value = kv
            if key not in acc:
                acc[key] = []
            acc[key].append(value)
            return acc

        return reduce(reducer, kv_stream, {})