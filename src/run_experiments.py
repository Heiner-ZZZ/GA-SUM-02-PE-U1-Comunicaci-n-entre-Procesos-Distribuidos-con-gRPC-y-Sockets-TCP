import argparse
import hashlib
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from src.common.config import (
    BENCHMARK_REPETITIONS,
    DATA_DIR,
    MESSAGES_PER_RUN,
    NODES,
    PAYLOAD_SIZES,
    WARMUP_MESSAGES,
)
from src.common.machine_info import save_machine_info
from src.grpc import client_grpc
from src.sockets import client as client_tcp


def _is_port_open(host: str, port: int, timeout: float = 0.2) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _wait_ports_ready(addresses: Dict[str, Tuple[str, int]], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if all(_is_port_open(host, port) for host, port in addresses.values()):
            return
        time.sleep(0.1)
    pending = [n for n, (h, p) in addresses.items() if not _is_port_open(h, p)]
    raise TimeoutError(f"Los siguientes nodos no respondieron a tiempo: {pending}")


def _wait_ports_closed(addresses: Dict[str, Tuple[str, int]], timeout: float = 8.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if all(not _is_port_open(host, port) for host, port in addresses.values()):
            return
        time.sleep(0.1)


def _start_node_processes(
    module_name: str,
    addresses: Dict[str, Tuple[str, int]],
    verbose: bool = False,
) -> List[subprocess.Popen]:
    procs: List[subprocess.Popen] = []
    for node_name, (host, port) in addresses.items():
        cmd = [
            sys.executable,
            "-m",
            module_name,
            "--node",
            node_name,
            "--host",
            host,
            "--port",
            str(port),
        ]
        if not verbose:
            cmd.append("--quiet")
        procs.append(subprocess.Popen(cmd))
    try:
        _wait_ports_ready(addresses)
    except Exception:
        _stop_node_processes(procs, addresses)
        raise
    return procs


def _stop_node_processes(
    procs: Sequence[subprocess.Popen],
    addresses: Dict[str, Tuple[str, int]],
) -> None:
    for proc in procs:
        if proc.poll() is None:
            proc.terminate()
    for proc in procs:
        try:
            proc.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2.0)
    _wait_ports_closed(addresses)


def compute_sha256(file_path: Path) -> str:
    h = hashlib.sha256()
    with file_path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def write_sha256sums(files: Sequence[Path], output_path: Path) -> Path:
    ordered = sorted((p for p in files if p.exists()), key=lambda p: p.name)
    lines = [f"{compute_sha256(p)}  {p.name}" for p in ordered]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_path


def run_protocol_for_mechanism(
    mechanism: str,
    output_dir: Path,
    addresses: Dict[str, Tuple[str, int]],
    sizes: Dict[str, int],
    repetitions: int,
    messages_per_run: int,
    warmup_messages: int,
    external_servers: bool = False,
    verbose: bool = False,
) -> List[Path]:
    if mechanism == "sockets":
        server_module = "src.sockets.server"
        client_mod = client_tcp
    elif mechanism == "grpc":
        server_module = "src.grpc.server_grpc"
        client_mod = client_grpc
    else:
        raise ValueError(f"Mecanismo no soportado: {mechanism}")

    procs: List[subprocess.Popen] = []
    try:
        if not external_servers:
            print(f"[{mechanism}] Iniciando 3 procesos de servidor en puertos 5000, 5001 y 5002...")
            procs = _start_node_processes(server_module, addresses, verbose=verbose)
        else:
            _wait_ports_ready(addresses)

        print(f"[{mechanism}] Ejecutando 20 intercambios con reloj de Lamport...")
        events = client_mod.run_lamport_exchanges(addresses)
        ev_csv = client_mod.save_events_csv(events, output_dir / f"events_{mechanism}.csv")
        ev_json = client_mod.save_events_json(events, output_dir / f"events_{mechanism}.json")

        print(
            f"[{mechanism}] Ejecutando benchmark "
            f"(calentamiento={warmup_messages}, {repetitions}x{messages_per_run}, "
            f"tamanos={list(sizes.keys())}, secuencial + concurrente)..."
        )
        t_start = time.perf_counter()
        latency_rows = client_mod.run_latency_benchmark(
            addresses=addresses,
            sizes=sizes,
            repetitions=repetitions,
            messages_per_run=messages_per_run,
            warmup_messages=warmup_messages,
            include_concurrent=True,
        )
        elapsed = time.perf_counter() - t_start
        lat_csv = client_mod.save_latency_csv(latency_rows, output_dir / f"latency_{mechanism}.csv")
        valid_count = sum(1 for r in latency_rows if not r["is_warmup"])
        print(
            f"[{mechanism}] Completado en {elapsed:.2f} s | "
            f"eventos={len(events)} | muestras_latencia={len(latency_rows)} ({valid_count} validas)"
        )
        return [ev_csv, ev_json, lat_csv]
    finally:
        if procs:
            _stop_node_processes(procs, addresses)


def run_all(
    output_dir: Path = DATA_DIR,
    mechanism: str = "all",
    sizes: Optional[Dict[str, int]] = None,
    repetitions: int = BENCHMARK_REPETITIONS,
    messages_per_run: int = MESSAGES_PER_RUN,
    warmup_messages: int = WARMUP_MESSAGES,
    external_servers: bool = False,
    verbose: bool = False,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    size_map = sizes or dict(PAYLOAD_SIZES)

    machine_file = save_machine_info(output_dir / "machine_info.json")
    print(f"[exp] Condiciones de la maquina registradas en {machine_file}")

    generated: List[Path] = [machine_file]
    mechanisms = ("sockets", "grpc") if mechanism == "all" else (mechanism,)

    for mech in mechanisms:
        files = run_protocol_for_mechanism(
            mechanism=mech,
            output_dir=output_dir,
            addresses=NODES,
            sizes=size_map,
            repetitions=repetitions,
            messages_per_run=messages_per_run,
            warmup_messages=warmup_messages,
            external_servers=external_servers,
            verbose=verbose,
        )
        generated.extend(files)

    # Incluye todos los archivos crudos presentes en output_dir si se ejecuto por partes
    tracked_names = (
        "events_sockets.csv",
        "events_sockets.json",
        "events_grpc.csv",
        "events_grpc.json",
        "latency_sockets.csv",
        "latency_grpc.csv",
        "machine_info.json",
    )
    all_raw_files = [output_dir / name for name in tracked_names if (output_dir / name).exists()]
    sums_path = write_sha256sums(all_raw_files, output_dir / "SHA256SUMS")
    print(f"[exp] Integridad SHA-256 guardada en {sums_path}:")
    print(sums_path.read_text(encoding="utf-8").strip())
    return sums_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Orquestador experimental completo para Sockets TCP y gRPC."
    )
    parser.add_argument("--mechanism", choices=["all", "sockets", "grpc"], default="all")
    parser.add_argument("--runs", type=int, default=BENCHMARK_REPETITIONS)
    parser.add_argument("--messages", type=int, default=MESSAGES_PER_RUN)
    parser.add_argument("--warmup", type=int, default=WARMUP_MESSAGES)
    parser.add_argument("--payload-bytes", type=int, default=None)
    parser.add_argument("--output-dir", type=Path, default=DATA_DIR)
    parser.add_argument(
        "--external-servers",
        action="store_true",
        help="Usa servidores ya iniciados manualmente en 5000, 5001 y 5002.",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    sizes = {f"{args.payload_bytes}B": args.payload_bytes} if args.payload_bytes else None
    run_all(
        output_dir=args.output_dir,
        mechanism=args.mechanism,
        sizes=sizes,
        repetitions=args.runs,
        messages_per_run=args.messages,
        warmup_messages=args.warmup,
        external_servers=args.external_servers,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
