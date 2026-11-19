import json
import socket
import struct
from typing import Any, Dict, Optional

HEADER_FORMAT = "!I"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
MAX_FRAME_BYTES = 16 * 1024 * 1024


def recv_exact(sock: socket.socket, num_bytes: int) -> Optional[bytes]:
    chunks = bytearray()
    while len(chunks) < num_bytes:
        data = sock.recv(num_bytes - len(chunks))
        if not data:
            if not chunks:
                return None
            raise ConnectionError("Conexion cerrada antes de completar el mensaje.")
        chunks.extend(data)
    return bytes(chunks)


def send_json(sock: socket.socket, payload: Dict[str, Any]) -> None:
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    # Prefijo de 4 bytes con la longitud para evitar fragmentacion TCP en payloads grandes (10 KB+)
    header = struct.pack(HEADER_FORMAT, len(body))
    sock.sendall(header + body)


def recv_json(sock: socket.socket) -> Optional[Dict[str, Any]]:
    raw_header = recv_exact(sock, HEADER_SIZE)
    if raw_header is None:
        return None
    (length,) = struct.unpack(HEADER_FORMAT, raw_header)
    if length == 0 or length > MAX_FRAME_BYTES:
        raise ValueError(f"Tamano de trama invalido: {length} bytes.")
    raw_body = recv_exact(sock, length)
    if raw_body is None:
        raise ConnectionError("Conexion cerrada al leer el cuerpo del mensaje.")
    return json.loads(raw_body.decode("utf-8"))
