from .custom_types import IOModule, IOType


class IOOP:
    def __init__(
        self, mod: IOModule, type: IOType, rank, start_time, end_time, offset, length
    ):
        assert start_time < end_time, (
            f"start_time >= end_time: {start_time} >= {end_time}"
        )
        self.mod = mod
        self.type = type
        self.rank = rank
        self.length = length
        self.start_offset = offset
        self.end_offset = offset + length
        self.start_time = start_time
        self.end_time = end_time

    @property
    def duration(self):
        return self.end_time - self.start_time

    @property
    def bandwidth(self):
<<<<<<< HEAD
        assert self.duration > 0
        return self.length / self.duration if self.duration > 0 else 0
=======
        assert self.duration() > 0
        return self.length / self.duration() if self.duration() > 0 else 0
>>>>>>> 70b303be2cdf5dfa5b27c4c446d50c57fa7b1d0b

    def __str__(self):
        return (f"{self.mod.value} - {self.type.value} - {self.rank} - "
                f"({self.start_time}, {self.end_time}) - "
                f"({self.start_offset}, {self.end_offset}) - "
                f"length={self.length} duration={self.duration}")
