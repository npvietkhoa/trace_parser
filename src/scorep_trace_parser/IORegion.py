from __future__ import annotations
from typing import Dict
from .IOOP import IOOP
from .custom_types import custom_any

class IORegion:
    #TODO: Implement IORegion class to represent a region of IO operations.
    # Region can contains ioop - i/o operation
    # Score-P records regions and IO operations separately.
    def __init__(self, name, rank, start_time, end_time, ioop: IOOP = None):
        assert start_time < end_time, "Start time must be less than end time"
        self.name = name
        self.rank = rank
        self.start_time = start_time
        self.end_time = end_time
        self.ioop = ioop

    @property
    def duration(self):
        return self.end_time - self.start_time
    
    def to_dict(self) -> Dict[str, custom_any]:
        return {
            'name': self.name,
            'rank': self.rank,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration': self.duration,
        } | (self.ioop.to_dict())