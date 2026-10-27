"""Private benchmark diagnostic. Read IPv4 TCP_INFO through Linux inet_diag."""

import json
import socket
import struct
import time


def dump():
    rows = []
    with socket.socket(socket.AF_NETLINK, socket.SOCK_DGRAM, 4) as sock:
        sock.settimeout(2)
        request = struct.pack("=BBBBI", socket.AF_INET, socket.IPPROTO_TCP, 2, 0, 2) + bytes(48)
        sock.sendto(struct.pack("=IHHII", len(request) + 16, 20, 0x301, 1, 0) + request, (0, 0))
        while True:
            data = sock.recv(65535)
            offset = 0
            while offset + 16 <= len(data):
                length, kind, flags, seq, pid = struct.unpack_from("=IHHII", data, offset)
                if length < 16 or offset + length > len(data):
                    raise ValueError("Malformed inet_diag message")
                payload = data[offset + 16 : offset + length]
                if kind == 3:
                    return rows
                if kind == 2:
                    raise OSError(struct.unpack_from("=i", payload)[0], "inet_diag failed")
                if kind == 20 and len(payload) >= 72:
                    local_port, remote_port = struct.unpack_from("!HH", payload, 4)
                    row = {
                        "local_port": local_port, "remote_port": remote_port,
                        "local": socket.inet_ntop(socket.AF_INET, payload[8:12]),
                        "remote": socket.inet_ntop(socket.AF_INET, payload[24:28]),
                        "receive_queue": struct.unpack_from("=I", payload, 56)[0],
                        "send_queue": struct.unpack_from("=I", payload, 60)[0],
                    }
                    pos = 72
                    while pos + 4 <= len(payload):
                        size, typ = struct.unpack_from("=HH", payload, pos)
                        if size < 4 or pos + size > len(payload):
                            raise ValueError("Malformed inet_diag attribute")
                        info = payload[pos + 4 : pos + size]
                        if typ & 0x3fff == 2 and len(info) >= 104:
                            row.update(rtt_us=struct.unpack_from("=I", info, 68)[0],
                                       rttvar_us=struct.unpack_from("=I", info, 72)[0],
                                       total_retransmits=struct.unpack_from("=I", info, 100)[0])
                        pos += (size + 3) & ~3
                    rows.append(row)
                offset += (length + 3) & ~3


def check():
    # Validate the ABI offsets against getsockopt on a task-owned loopback pair.
    with socket.socket() as listener, socket.socket() as client:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        client.connect(listener.getsockname())
        with listener.accept()[0]:
            direct = struct.unpack_from("=I", client.getsockopt(socket.IPPROTO_TCP, socket.TCP_INFO, 104), 68)[0]
            row = next(r for r in dump() if r["local_port"] == client.getsockname()[1] and r["remote_port"] == listener.getsockname()[1])
            if row.get("rtt_us") != direct:
                raise ValueError("inet_diag and TCP_INFO RTT differ")


if __name__ == "__main__":
    check()
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline:
        print(json.dumps({"observed_at": time.time(), "abi_check": "passed",
                          "flows": [r for r in dump() if r["remote_port"] == 6875]}), flush=True)
        time.sleep(5)
