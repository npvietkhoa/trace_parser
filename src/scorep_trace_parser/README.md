OTF2 IoOperationBegin - IoOperationComplete
- io_handle:
    - `io_handle`: 

Each OTF2:
    - definition: (`trace.definitions`): handle
    - log of location and event (`trace.events`):

Each event `IoOpreationBegin`:
    - `time`: 3001736103573271, - in tick
    - `attributes`: None ...
    - `handle`: IoHandle [1] 'STDOUT_FILENO' - ref. to handle 
    - `mode`: IoOperationMode.WRITE, 
    - `operation_flags`': IoOperationFlag.NONE, 
    - `bytes_request`: 41 - TODO: is this always equal the byte_result? 
    - `matching_id`: 1 - Must be match with the Complete.

Each event `IoOperationComplete`:
    - `time`: 3001736359087503, - in tick
    - `attributes`: None, 
    - `handle`: IoHandle [1] 'STDOUT_FILENO', '
    - `bytes_result`: 16, 
    - `matching_id`: 1 - must be match with one from Begin.

Each Handle:
    - name: ...
    - io_paradigm': IoParadigm [0]

Each IoParadigm:
    -  
    
