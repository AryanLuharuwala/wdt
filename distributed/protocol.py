"""
Network protocol for peer-to-peer shard transfer.
Uses a simple HTTP-based API on each peer for shard upload/download.
"""
import hashlib
import json
import logging
import os
import ssl
import struct
from enum import IntEnum
from typing import Optional

logger = logging.getLogger("wdt.protocol")

# Message types for the binary protocol used in direct socket transfer
class MessageType(IntEnum):
    HANDSHAKE = 0x01
    HANDSHAKE_ACK = 0x02
    SHARD_REQUEST = 0x10
    SHARD_RESPONSE = 0x11
    SHARD_PUSH = 0x12
    SHARD_ACK = 0x13
    FILE_META_REQUEST = 0x20
    FILE_META_RESPONSE = 0x21
    ERROR = 0xFF


# Header: [type:1][payload_length:4]
HEADER_SIZE = 5
MAX_PAYLOAD_SIZE = 64 * 1024 * 1024  # 64MB max payload


def encode_message(msg_type: MessageType, payload: bytes) -> bytes:
    """Encode a protocol message with header."""
    if len(payload) > MAX_PAYLOAD_SIZE:
        raise ValueError(f"Payload too large: {len(payload)} > {MAX_PAYLOAD_SIZE}")
    header = struct.pack("!BI", msg_type.value, len(payload))
    return header + payload


def decode_header(data: bytes):
    """Decode a message header. Returns (msg_type, payload_length)."""
    if len(data) < HEADER_SIZE:
        raise ValueError("Insufficient data for header")
    msg_type, payload_len = struct.unpack("!BI", data[:HEADER_SIZE])
    return MessageType(msg_type), payload_len


def recv_exact(sock, n: int) -> bytes:
    """Receive exactly n bytes from a socket."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(min(n - len(buf), 65536))
        if not chunk:
            raise ConnectionError("Connection closed while receiving data")
        buf.extend(chunk)
    return bytes(buf)


def recv_message(sock):
    """Receive a full protocol message. Returns (msg_type, payload_bytes)."""
    header = recv_exact(sock, HEADER_SIZE)
    msg_type, payload_len = decode_header(header)
    if payload_len > MAX_PAYLOAD_SIZE:
        raise ValueError(f"Payload size {payload_len} exceeds maximum")
    payload = recv_exact(sock, payload_len) if payload_len > 0 else b""
    return msg_type, payload


def send_message(sock, msg_type: MessageType, payload: bytes):
    """Send a protocol message over a socket."""
    data = encode_message(msg_type, payload)
    sock.sendall(data)


def make_handshake(peer_id: str, token: str) -> bytes:
    """Create a handshake payload."""
    return json.dumps({"peer_id": peer_id, "token": token}).encode()


def parse_handshake(payload: bytes) -> dict:
    """Parse a handshake payload."""
    return json.loads(payload.decode())


def make_shard_request(shard_id: str) -> bytes:
    """Create a shard request payload."""
    return json.dumps({"shard_id": shard_id}).encode()


def parse_shard_request(payload: bytes) -> dict:
    return json.loads(payload.decode())


def make_shard_response(shard_id: str, data: bytes) -> bytes:
    """Create a shard response: JSON header + raw shard data."""
    header = json.dumps({
        "shard_id": shard_id,
        "size": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }).encode()
    # [header_len:4][header][shard_data]
    return struct.pack("!I", len(header)) + header + data


def parse_shard_response(payload: bytes):
    """Parse a shard response. Returns (metadata_dict, shard_data)."""
    header_len = struct.unpack("!I", payload[:4])[0]
    header = json.loads(payload[4:4 + header_len].decode())
    data = payload[4 + header_len:]
    return header, data


def make_file_meta_request(file_id: str) -> bytes:
    return json.dumps({"file_id": file_id}).encode()


def make_file_meta_response(metadata: dict) -> bytes:
    return json.dumps(metadata).encode()


def make_error(code: int, message: str) -> bytes:
    return json.dumps({"code": code, "message": message}).encode()
