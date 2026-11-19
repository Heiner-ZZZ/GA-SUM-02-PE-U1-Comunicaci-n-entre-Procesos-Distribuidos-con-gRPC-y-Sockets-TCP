import argparse
import csv
import json
import socket
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from src.common.config import (
    BENCHMARK_REPETITIONS,
    DATA_DIR,
    EVENT_CSV_FIELDS,
    LATENCY_CSV_FIELDS,
    MESSAGES_PER_RUN,
    NODES,
    PAYLOAD_SIZES,
    WARMUP_MESSAGES,
    build_payload,
)
from src.common.lamport import LamportClock
from src.sockets.framing import recv_json, send_json

NODE_NAMES = ("nodo-A", "nodo-B", "nodo-C")

# Secuencia de 20 intercambios entre los 3 nodos.
# Cada elemento es una lista de envios ejecutados en paralelo (1 = secuencial, 2 = concurrente real).
EXCHANGE_STEPS: Sequence[Sequence[Tuple[str, str, str]]] = (
    (("ex-01", "nodo-A", "nodo-C"), ("ex-02", "nodo-B", "nodo-C")),
    (("ex-03", "nodo-C", "nodo-A"),),
    (("ex-04", "nodo-A", "nodo-B"),),
    (("ex-05", "nodo-B", "nodo-C"),),
    (("ex-06", "nodo-C", "nodo-B"),),
    (("ex-07", "nodo-B", "nodo-A"),),
    (("ex-08", "nodo-A", "nodo-C"),),
    (("ex-09", "nodo-A", "nodo-B"), ("ex-10", "nodo-C", "nodo-B")),
    (("ex-11", "nodo-B", "nodo-A"),),
    (("ex-12", "nodo-A", "nodo-C"),),
    (("ex-13", "nodo-C", "nodo-B"),),
    (("ex-14", "nodo-B", "nodo-C"),),
    (("ex-15", "nodo-C", "nodo-A"),),
    (("ex-16", "nodo-A", "nodo-B"),),
    (("ex-17", "nodo-B", "nodo-A"),),
    (("ex-18", "nodo-A", "nodo-C"),),
    (("ex-19", "nodo-C", "nodo-B"),),
    (("ex-20", "nodo-B", "nodo-C"),),
)


def _open_tcp_connection(host: str, port: int, timeout: float = 5.0) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    sock.connect((host, port))
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return sock


def _sync_clock_to(clock: LamportClock, target_value: int, lock: threading.Lock) -> None:
    with lock:
        if target_value > clock.value:
            # update(target_value - 1) lleva el reloj exactamente a target_value si local < target_value
            clock.update(target_value - 1)


def _reset_servers(addresses: Dict[str, Tuple[str, int]]) -> None:
    for node_name, (host, port) in addresses.items():
        with _open_tcp_connection(host, port) as sock:
            send_json(sock, {"control": "reset_clock"})
            resp = recv_json(sock)
            if not resp or resp.get("status") != "OK":
                raise RuntimeError(f"No se pudo reiniciar el reloj en {node_name}: {resp}")


def _fetch_server_events(addresses: Dict[str, Tuple[str, int]]) -> List[Dict[str, Any]]:
    collected: List[Dict[str, Any]] = []
    for node_name, (host, port) in addresses.items():
        with _open_tcp_connection(host, port) as sock:
            send_json(sock, {"control": "get_events"})
            resp = recv_json(sock)
            if not resp or resp.get("status") != "OK":
                raise RuntimeError(f"No se pudieron leer eventos de {node_name}: {resp}")
            collected.extend(resp.get("events", []))
    return collected


def run_lamport_exchanges(
    addresses: Optional[Dict[str, Tuple[str, int]]] = None,
) -> List[Dict[str, Any]]:
    targets = addresses or NODES
    _reset_servers(targets)

    clocks = {name: LamportClock(name) for name in NODE_NAMES}
    sync_locks = {name: threading.Lock() for name in NODE_NAMES}
    send_events: List[Dict[str, Any]] = []
    send_events_lock = threading.Lock()
    errors: List[Exception] = []

    def execute_send(
        msg_id: str,
        sender: str,
        receiver: str,
        barrier: Optional[threading.Barrier] = None,
    ) -> None:
        try:
            host, port = targets[receiver]
            with _open_tcp_connection(host, port) as sock:
                if barrier is not None:
                    barrier.wait()
                t_send = clocks[sender].tick()
                wall_time = datetime.now(timezone.utc).isoformat()
                with send_events_lock:
                    send_events.append(
                        {
                            "mechanism": "sockets",
                            "msg_id": msg_id,
                            "node": sender,
                            "peer": receiver,
                            "event_type": "send",
                            "lamport_clock": t_send,
                            "wall_time_iso": wall_time,
                        }
                    )
                send_json(
                    sock,
                    {
                        "sender": sender,
                        "timestamp": t_send,
                        "message": f"intercambio-{msg_id}",
                        "msg_id": msg_id,
                        "record_event": True,
                    },
                )
                ack = recv_json(sock)
                if not ack or ack.get("status") != "OK":
                    raise RuntimeError(f"Acuse invalido para {msg_id}: {ack}")
                _sync_clock_to(clocks[receiver], int(ack["timestamp"]), sync_locks[receiver])
        except Exception as exc:
            errors.append(exc)

    for step in EXCHANGE_STEPS:
        if len(step) == 1:
            msg_id, sender, receiver = step[0]
            execute_send(msg_id, sender, receiver)
        else:
            barrier = threading.Barrier(len(step))
            threads = [
                threading.Thread(
                    target=execute_send,
                    args=(msg_id, sender, receiver, barrier),
                )
                for msg_id, sender, receiver in step
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        if errors:
            raise errors[0]

    recv_events = _fetch_server_events(targets)
    all_events = send_events + recv_events
    all_events.sort(
        key=lambda e: (
            e["wall_time_iso"],
            int(e["lamport_clock"]),
            0 if e["event_type"] == "send" else 1,
        )
    )
    return all_events


def _run_sequential_phase(
    targets: Dict[str, Tuple[str, int]],
    size_label: str,
    size_bytes: int,
    repetitions: int,
    messages_per_run: int,
    warmup_messages: int,
) -> List[Dict[str, Any]]:
    payload = build_payload(size_bytes, prefix=f"TCP-{size_label}:")
    clock = LamportClock("nodo-A")
    rows: List[Dict[str, Any]] = []

    conns = {name: _open_tcp_connection(*targets[name]) for name in NODE_NAMES}
    try:
        for run_id in range(0, repetitions + 1):
            is_warmup = run_id == 0
            count = warmup_messages if is_warmup else messages_per_run
            for seq in range(count):
                receiver = NODE_NAMES[seq % len(NODE_NAMES)]
                ts = clock.tick()
                msg = {
                    "sender": "nodo-A",
                    "timestamp": ts,
                    "message": payload,
                    "msg_id": f"seq-{size_label}-r{run_id}-{seq}",
                    "record_event": False,
                }
                t0 = time.perf_counter()
                send_json(conns[receiver], msg)
                ack = recv_json(conns[receiver])
                t1 = time.perf_counter()
                if not ack or ack.get("status") != "OK":
                    raise RuntimeError(f"Error en envio TCP secuencial: {ack}")

                rows.append(
                    {
                        "mechanism": "sockets",
                        "mode": "sequential",
                        "size_label": size_label,
                        "payload_bytes": size_bytes,
                        "run_id": run_id,
                        "is_warmup": is_warmup,
                        "seq": seq,
                        "sender": "nodo-A",
                        "receiver": receiver,
                        "latency_ms": (t1 - t0) * 1000.0,
                        "sent_perf_s": t0,
                        "recv_perf_s": t1,
                    }
                )
    finally:
        for sock in conns.values():
            sock.close()

    return rows


def _run_concurrent_phase(
    targets: Dict[str, Tuple[str, int]],
    size_label: str,
    size_bytes: int,
    repetitions: int,
    messages_per_run: int,
    warmup_messages: int,
) -> List[Dict[str, Any]]:
    payload = build_payload(size_bytes, prefix=f"TCPC-{size_label}:")
    all_rows: List[Dict[str, Any]] = []

    for run_id in range(0, repetitions + 1):
        is_warmup = run_id == 0
        count = warmup_messages if is_warmup else messages_per_run
        barrier = threading.Barrier(len(NODE_NAMES))
        run_rows: List[Dict[str, Any]] = []
        rows_lock = threading.Lock()
        errors: List[Exception] = []

        def client_worker(sender_name: str) -> None:
            peers = [n for n in NODE_NAMES if n != sender_name]
            clock = LamportClock(sender_name)
            local_rows: List[Dict[str, Any]] = []
            conns = {peer: _open_tcp_connection(*targets[peer]) for peer in peers}
            try:
                barrier.wait()
                for seq in range(count):
                    receiver = peers[seq % len(peers)]
                    ts = clock.tick()
                    msg = {
                        "sender": sender_name,
                        "timestamp": ts,
                        "message": payload,
                        "msg_id": f"conc-{size_label}-r{run_id}-{sender_name}-{seq}",
                        "record_event": False,
                    }
                    t0 = time.perf_counter()
                    send_json(conns[receiver], msg)
                    ack = recv_json(conns[receiver])
                    t1 = time.perf_counter()
                    if not ack or ack.get("status") != "OK":
                        raise RuntimeError(f"Error en envio TCP concurrente: {ack}")

                    local_rows.append(
                        {
                            "mechanism": "sockets",
                            "mode": "concurrent",
                            "size_label": size_label,
                            "payload_bytes": size_bytes,
                            "run_id": run_id,
                            "is_warmup": is_warmup,
                            "seq": seq,
                            "sender": sender_name,
                            "receiver": receiver,
                            "latency_ms": (t1 - t0) * 1000.0,
                            "sent_perf_s": t0,
                            "recv_perf_s": t1,
                        }
                    )
                with rows_lock:
                    run_rows.extend(local_rows)
            except Exception as exc:
                errors.append(exc)
            finally:
                for sock in conns.values():
                    sock.close()

        threads = [threading.Thread(target=client_worker, args=(n,)) for n in NODE_NAMES]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        if errors:
            raise errors[0]

        all_rows.extend(run_rows)

    return all_rows


def run_latency_benchmark(
    addresses: Optional[Dict[str, Tuple[str, int]]] = None,
    sizes: Optional[Dict[str, int]] = None,
    repetitions: int = BENCHMARK_REPETITIONS,
    messages_per_run: int = MESSAGES_PER_RUN,
    warmup_messages: int = WARMUP_MESSAGES,
    include_concurrent: bool = True,
) -> List[Dict[str, Any]]:
    targets = addresses or NODES
    size_map = sizes or PAYLOAD_SIZES
    rows: List[Dict[str, Any]] = []

    for size_label, size_bytes in size_map.items():
        rows.extend(
            _run_sequential_phase(
                targets, size_label, size_bytes, repetitions, messages_per_run, warmup_messages
            )
        )
        if include_concurrent:
            rows.extend(
                _run_concurrent_phase(
                    targets, size_label, size_bytes, repetitions, messages_per_run, warmup_messages
                )
            )
    return rows


def save_events_csv(events: Sequence[Dict[str, Any]], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=EVENT_CSV_FIELDS)
        writer.writeheader()
        writer.writerows(events)
    return path


def save_events_json(events: Sequence[Dict[str, Any]], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(list(events), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return path


def save_latency_csv(rows: Sequence[Dict[str, Any]], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=LATENCY_CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _parse_sizes(sizes_arg: Optional[str], custom_bytes: Optional[int]) -> Dict[str, int]:
    if custom_bytes is not None:
        return {f"{custom_bytes}B": custom_bytes}
    if not sizes_arg:
        return dict(PAYLOAD_SIZES)
    selected: Dict[str, int] = {}
    for token in sizes_arg.split(","):
        key = token.strip()
        if key in PAYLOAD_SIZES:
            selected[key] = PAYLOAD_SIZES[key]
        else:
            raise ValueError(f"Tamano desconocido '{key}'. Opciones: {list(PAYLOAD_SIZES.keys())}")
    return selected


def main() -> None:
    parser = argparse.ArgumentParser(description="Cliente TCP para intercambios Lamport y benchmark.")
    parser.add_argument("--exchanges", action="store_true", help="Ejecutar los 20 intercambios Lamport.")
    parser.add_argument("--benchmark", action="store_true", help="Ejecutar mediciones de latencia RTT.")
    parser.add_argument("--sizes", default=None, help="Lista separada por coma: 100B,10KB.")
    parser.add_argument("--payload-bytes", type=int, default=None, help="Tamano personalizado en bytes.")
    parser.add_argument("--runs", type=int, default=BENCHMARK_REPETITIONS)
    parser.add_argument("--messages", type=int, default=MESSAGES_PER_RUN)
    parser.add_argument("--warmup", type=int, default=WARMUP_MESSAGES)
    parser.add_argument("--no-concurrent", action="store_true")
    parser.add_argument("--events-csv", type=Path, default=DATA_DIR / "events_sockets.csv")
    parser.add_argument("--events-json", type=Path, default=DATA_DIR / "events_sockets.json")
    parser.add_argument("--latency-csv", type=Path, default=DATA_DIR / "latency_sockets.csv")
    args = parser.parse_args()

    run_both = not args.exchanges and not args.benchmark

    if args.exchanges or run_both:
        events = run_lamport_exchanges()
        save_events_csv(events, args.events_csv)
        save_events_json(events, args.events_json)
        print(f"[sockets] 20 intercambios completados ({len(events)} eventos) -> {args.events_csv}")

    if args.benchmark or run_both:
        size_map = _parse_sizes(args.sizes, args.payload_bytes)
        rows = run_latency_benchmark(
            sizes=size_map,
            repetitions=args.runs,
            messages_per_run=args.messages,
            warmup_messages=args.warmup,
            include_concurrent=not args.no_concurrent,
        )
        save_latency_csv(rows, args.latency_csv)
        valid = [r["latency_ms"] for r in rows if not r["is_warmup"]]
        avg_ms = sum(valid) / len(valid) if valid else 0.0
        print(
            f"[sockets] Benchmark completado ({len(rows)} filas totales, "
            f"{len(valid)} validas, media global={avg_ms:.4f} ms) -> {args.latency_csv}"
        )


if __name__ == "__main__":
    main()
