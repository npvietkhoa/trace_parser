import os
import otf2
import warnings
from .IOOP import IOOP
from .custom_types import IOMod, IOParadigm
from collections import defaultdict
from itertools import chain, pairwise
from typing import Callable, Dict, Tuple, Any
from functools import cached_property, reduce


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

    @cached_property
    def parse_trace(self) -> dict[otf2.definitions.Location, list[IOOP]]:
        """
        Parse the trace file and extract IOOP events.
        Cached after the first call per instance.
        """
        event_stack = defaultdict(list)      # for Enter/Leave regions
        ioop_stack = defaultdict(list)
        skip_event_type = set()
        with otf2.reader.open(self.fp) as trace:
            for loc, event in trace.events:
                if isinstance(event, otf2.events.Enter):
                    event_stack[loc].append(event)
                elif isinstance(event, otf2.events.Leave):
                    assert event_stack[loc], "Empty stack!"
                    assert isinstance(event_stack[loc][-1], otf2.events.Enter), "Must be an enter to leave"
                    assert event_stack[loc][-1].region.name == event.region.name, f"{event_stack[loc][-1].region.name} != {event.region.name} - {event.time}: Leave event region does not match Enter event region"
                    event_stack[loc].pop()
                elif isinstance(event, otf2.events.IoOperationBegin):
                    assert event_stack[loc], "I/O region must be in other region"
                    assert isinstance(event_stack[loc][-1], otf2.events.Enter), "IoOperationBegin must follow an Enter event"
                    event_stack[loc].append(event)
                elif isinstance(event, otf2.events.IoOperationComplete):
                    assert event_stack[loc], "I/O region must be in other region"
                    assert isinstance(event_stack[loc][-1], otf2.events.IoOperationBegin), "IO completes must have a begin"
                    matching_io_events = [
                        e for e in event_stack[loc]
                        if isinstance(e, otf2.events.IoOperationBegin) and 
                        e.matching_id == event.matching_id and
                        e.handle.io_paradigm == event.handle.io_paradigm
                    ]
                    assert len(matching_io_events) == 1, "Multiple IoOperationBegin events found for the same IoOperationEnd"

                    start_io_event = matching_io_events[0]

                    event_stack[loc].remove(start_io_event)

                    # Get event name for this IO operation
                    assert event_stack[loc], "IoOperationEnd without matching Enter event"
                    start_event = event_stack[loc][-1]

                    ioop = IOOP(
                        fname=start_event.region.name,  # region name as function name
                        mod=IOMod.from_otf2(start_io_event.mode),
                        paradigm=IOParadigm.from_otf2(start_io_event.handle),
                        start_time=start_io_event.time,
                        end_time=event.time,
                        bytes_request=start_io_event.bytes_request,
                        bytes_result=event.bytes_result
                    )
                    ioop_stack[loc].append(ioop)
                else:
                    skip_event_type.add(type(event))
            print(f"Skipped event types: {skip_event_type}")
        return ioop_stack
    
    @cached_property
    def overlapped(self) -> dict[otf2.definitions.Location, list[IOOP]]:
        """
        Returns a dictionary mapping each rank/location to a list of IOOPs
        where overlapping operations are split and adjusted.
        """
        parsed_trace = self.parse_trace  # cached property returns dict[Location, list[IOOP]]
        overlapped_trace = {}
        io_mods = set(op.mod for ioop_list in parsed_trace.values() for op in ioop_list)
        for io_mod in io_mods:
            new_ioops = []
            for rank, ioop_lst in parsed_trace.items():
                ioop_lst = [op for op in ioop_lst if op.mod == io_mod]
                sorted_ioops = sorted(ioop_lst, key=lambda op: op.start_time)
                if not sorted_ioops:
                    overlapped_trace[rank] = new_ioops
                    continue

                current_op = sorted_ioops[0]

                for next_op in sorted_ioops[1:]:
                    if self.is_overlappable([current_op, next_op]):

                        # Split the overlapping IOOPs
                        split_ops = self.handle_overlap(current_op, next_op)
                        # Add all but the last piece
                        new_ioops.extend(split_ops[:-1])
                        # Carry forward the last piece for next comparison
                        current_op = split_ops[-1]
                    else:
                        new_ioops.append(current_op)
                        current_op = next_op

                # Append the last current_op
                new_ioops.append(current_op)
                overlapped_trace[rank] = new_ioops

        return overlapped_trace
        
    @staticmethod
    def handle_overlap(op1: IOOP, op2: IOOP) -> list[IOOP]:
        """
        Splits op1 around op2 if they overlap.
        Ensures the sum of op1_1 and op1_2 bytes_result == op1.bytes_result.
        Keeps op2 unchanged.
        """
        assert op1.end_time > op2.start_time, "op1 and op2 must overlap"
        print(f"op1: ({op1.start_time}, {op1.end_time}), {op1.mod}, {op1.paradigm}, ({op1.bytes_request}, {op1.bytes_result}))")
        print(f"op2: ({op2.start_time}, {op2.end_time}), {op2.mod}, {op2.paradigm}, ({op2.bytes_request}, {op2.bytes_result}))")
        # Durations of split parts (can be zero)
        pre_duration = max(0, op2.start_time - op1.start_time)
        post_duration = max(0, op1.end_time - op2.end_time)
        total_non_overlap = pre_duration + post_duration

        parts = []
        # Helper for proportional split
        def proportional(val: float, dur: float) -> float:
            return 0.0 if total_non_overlap == 0 else val * (dur / total_non_overlap)

        # Pre-overlap piece
        assert pre_duration > 0, "Pre-overlap duration must be positive because of overlap"
        parts.append(IOOP(
            fname=op1.fname,
            mod=op1.mod,
            paradigm=op1.paradigm,
            start_time=op1.start_time,
            end_time=op2.start_time,
            bytes_request=proportional(op1.bytes_request, pre_duration),
            bytes_result=proportional(op1.bytes_result, pre_duration)
        ))

        # Overlapping op2 kept as-is
        parts.append(op2)

        # Post-overlap piece
        if post_duration > 0:
            parts.append(IOOP(
                fname=op1.fname,
                mod=op1.mod,
                paradigm=op1.paradigm,
                start_time=op2.end_time,
                end_time=op1.end_time,
                bytes_request=proportional(op1.bytes_request, post_duration),
                bytes_result=proportional(op1.bytes_result, post_duration)
            ))
        for part in parts:
            print(f"Generated part: ({part.start_time} - {part.end_time}, {part.mod}, ({part.bytes_request}, {part.bytes_result}))")
        return parts

    @staticmethod
    def is_overlappable(ioop_lst: list[IOOP]) -> bool:
        """
        Returns True if any two IOOP objects in the list overlap in time.
        
        Args:
            ioop_lst: List of IOOP objects, assumed to have start_time and end_time attributes.
            
        Returns:
            True if any two IOOPs overlap, False otherwise.
        """
        if len(ioop_lst) < 2:
            return False

        # Sort IOOPs by start_time
        sorted_ioops = sorted(ioop_lst, key=lambda op: op.start_time)

        # Check overlaps in adjacent IOOPs using pairwise
        return any(a.end_time > b.start_time for a, b in pairwise(sorted_ioops))


        
    def aggregate_op_stat(
        self,
        stat_fn: Callable[[IOOP], Any]
    ) -> Dict[Tuple[IOMod, IOParadigm], list[Any]]:
        """
        Traverse all IOOP instances from parse_trace, apply stat_fn to each,
        and collect results in a dict keyed by (mod, paradigm).
        """
        # Flatten all IOOP lists from the location dictionary
        ops_stream = chain.from_iterable(self.parse_trace.values())

        # Map each IOOP to ((mod, paradigm), stat_fn(op))
        kv_stream = map(lambda op: ((op.mod, op.paradigm), stat_fn(op)), ops_stream)

        # Reduce into grouped dict
        def reducer(
            acc: Dict[Tuple[IOMod, IOParadigm], list[Any]],
            kv: Tuple[Tuple[IOMod, IOParadigm], Any]
        ) -> Dict[Tuple[IOMod, IOParadigm], list[Any]]:
            key, value = kv
            if key not in acc:
                acc[key] = []
            acc[key].append(value)
            return acc

        return reduce(reducer, kv_stream, {})