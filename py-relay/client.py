import socket
import struct
from proto_out import ygw_pb2

def build_message(link_id: int, msg_type: int, payload: bytes) -> bytes:
    version = 0
    node_id = 0
    header = struct.pack('>B B I I', version, msg_type, node_id, link_id)
    full_msg = header + payload
    length = len(full_msg)
    return struct.pack('>I', length) + full_msg

# Create parameter definitions
param1 = ygw_pb2.ParameterDef(
    name="temperature",
    type="float",
    unit="C",
)

param2 = ygw_pb2.ParameterDef(
    name="voltage",
    type="float",
    unit="V",
)

# Add them to a ParameterDefList
param_list = ygw_pb2.ParameterDefList()
param_list.parameters.extend([param1, param2])

# Serialize
payload = param_list.SerializeToString()

# Wrap with header
msg_type = ygw_pb2.PARAMETER_DEFS  # from the MessageType enum
link_id = 1  # your link ID
msg = build_message(link_id, msg_type, payload)

# Send via TCP
with socket.create_connection(("localhost", 7889)) as sock:
    sock.sendall(msg)
    print("ParameterDefList sent.")

