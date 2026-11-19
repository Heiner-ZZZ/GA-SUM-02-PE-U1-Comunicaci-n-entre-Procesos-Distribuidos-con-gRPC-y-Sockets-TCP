from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
ANALYSIS_DIR = ROOT_DIR / "analysis"
DOCS_FIGURES_DIR = ROOT_DIR / "docs" / "figures"

DEFAULT_HOST = "127.0.0.1"
NODES: Dict[str, Tuple[str, int]] = {
    "nodo-A": (DEFAULT_HOST, 5000),
    "nodo-B": (DEFAULT_HOST, 5001),
    "nodo-C": (DEFAULT_HOST, 5002),
}

EXCHANGES_COUNT = 20
WARMUP_MESSAGES = 100
BENCHMARK_REPETITIONS = 5
MESSAGES_PER_RUN = 100

SHORT_PAYLOAD_BYTES = 100
LONG_PAYLOAD_BYTES = 10 * 1024

PAYLOAD_SIZES: Dict[str, int] = {
    "100B": SHORT_PAYLOAD_BYTES,
    "10KB": LONG_PAYLOAD_BYTES,
}

EVENT_CSV_FIELDS = (
    "mechanism",
    "msg_id",
    "node",
    "peer",
    "event_type",
    "lamport_clock",
    "wall_time_iso",
)

LATENCY_CSV_FIELDS = (
    "mechanism",
    "mode",
    "size_label",
    "payload_bytes",
    "run_id",
    "is_warmup",
    "seq",
    "sender",
    "receiver",
    "latency_ms",
    "sent_perf_s",
    "recv_perf_s",
)


def get_node_address(node_name: str) -> Tuple[str, int]:
    if node_name not in NODES:
        valid = ", ".join(NODES.keys())
        raise ValueError(f"Nodo desconocido '{node_name}'. Validos: {valid}.")
    return NODES[node_name]


def build_payload(size_bytes: int, prefix: str = "") -> str:
    if size_bytes <= 0:
        raise ValueError("El tamano del payload debe ser positivo.")
    prefix_bytes = prefix.encode("utf-8")
    if len(prefix_bytes) >= size_bytes:
        return prefix_bytes[:size_bytes].decode("utf-8", errors="ignore")
    remaining = size_bytes - len(prefix_bytes)
    chunk = "0123456789ABCDEF"
    filler = (chunk * ((remaining // len(chunk)) + 1))[:remaining]
    return prefix + filler
