from __future__ import annotations
from .custom_types import IOMod, IOPradigm

class IOOP:
    def __init__(self, mod: IOMod, paradigm: IOPradigm, start_time, end_time, bytes_request, bytes_result):
        assert start_time < end_time, "Start time must be less than end time"
        self.mod = mod
        self.paradigm = paradigm
        self.start_time = start_time
        self.end_time = end_time
        self.bytes_request = bytes_request
        self.bytes_result = bytes_result

    @property
    def duration(self):
        return self.end_time - self.start_time
    
    @property
    def byte_rate(self):
        assert self.duration > 0, "Duration must be greater than zero"
        return self.bytes_request / self.duration


    def to_dict(self):
        return {
            'io_mod': self.mod.value,
            'io_start_time': self.start_time,
            'io_end_time': self.end_time,
            'io_duration': self.duration,
            'io_bytes_request': self.bytes_request,
            'io_byte_rate': self.byte_rate
        }