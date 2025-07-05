from recorder_viz import RecorderReader
from .custom_types import IOMod, IOPradigm, map_io_fn
from typing import List, Any

class IOOP:
    def __init__(self, fname: str, fid, start_time, end_time, call_depth: int, rargs: List[Any] = [], rlargs: List[Any] = []):
        assert start_time <= end_time, f"Start time {start_time} must be less than end time {end_time}"

        self.fname = fname
        self.fid = fid
        self.start_time = start_time
        self.end_time = end_time
        self.call_depth = call_depth
        self.rargs = rargs
        self.rlargs = rlargs
    
    @property
    def mod(self) -> IOMod:
        """
        Returns the IOMod of the operation.
        :return: The IOMod of the operation.
        :rtype: IOMod
        """
        mod, _ = map_io_fn(self.fname)
        return mod

    @property
    def paradigm(self) -> IOPradigm:
        """
        Returns the IOPradigm of the operation.
        :return: The IOPradigm of the operation.
        :rtype: IOPradigm
        """
        _, paradigm = map_io_fn(self.fname)
        return paradigm
    
    @property
    def duration(self) -> float:
        """
        Returns the duration of the operation.
        :return: The duration of the operation in seconds.
        :rtype: float
        """
        return self.end_time - self.start_time
    
