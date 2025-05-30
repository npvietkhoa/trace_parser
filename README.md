# Structure of a Darshan trace:
- report (trace):
    - record `DXT_POSIX`
      - log: rank 1: 
          - io_type write `write_segments {...}`
          - io_type read `read_segments {...}`
      - log: rank 2: ...
    - record `POSIX`
      - log: rank 1: 
          - io_type write `write_segments {...}`
          - io_type read `read_segments {...}`
    - record `DXT_MPIIO`
      - ...
    - record `MPIIO`
      - ...

# DXT Module Information:

| Field    | Description                                                                 |
|----------|-----------------------------------------------------------------------------|
| Module   | Corresponding DXT module (`DXT_POSIX` or `DXT_MPIIO`)                       |
| Rank     | Process rank responsible for the I/O operation                              |
| Wt/Rd    | Whether the operation was a write or read                                   |
| Segment  | The operation number for this segment (first operation is segment 0)        |
| Offset   | File offset where the I/O operation occurred                                |
| Length   | Length of the I/O operation in bytes                                        |
| Start    | Timestamp of the start of the operation (relative to application start time)|
| End      | Timestamp of the end of the operation (relative to application start time)  |

E.g. Using `darshan-dxt-parser` to parse a Darshan trace file, you might see output like this:
```aiignore
# Module    Rank  Wt/Rd  Segment          Offset       Length    Start(s)      End(s)
 X_MPIIO       2  write        0           81920         2621440      0.4517      0.4642
 X_MPIIO       2  write        1        10567680         2621440      0.6372      0.6476
 X_MPIIO       2  write        2        21053440         2621440      0.8213      0.8319
 X_MPIIO       2  write        3        31539200         2621440      1.0077      1.0183
 X_MPIIO       2  write        4        42024960         2621440      1.1928      1.2040
 ...
 ...
 X_MPIIO       2   read       29       304168960         2621440      7.9683      7.9721
 X_MPIIO       2   read       30       314654720         2621440      7.9731      7.9769
 X_MPIIO       2   read       31       325140480         2621440      7.9779      7.9817
 X_MPIIO       2   read       32       335626240         2621440      7.9827      7.9864
```