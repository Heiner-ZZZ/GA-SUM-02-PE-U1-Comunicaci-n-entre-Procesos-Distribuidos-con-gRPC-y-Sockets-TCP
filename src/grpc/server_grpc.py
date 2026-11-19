import argparse
import threading
from concurrent import futures
from datetime import datetime, timezone
from typing import List, Optional

import grpc

from src.common.config import get_node_address
from src.common.lamport import LamportClock
from src.grpc import messaging_pb2, messaging_pb2_grpc


class MessagingServicer(messaging_pb2_grpc.MessagingServiceServicer):
    def __init__(self, node_name: str, quiet: bool = False) -> None:
        self.node_name = node_name
        self.quiet = quiet
        self.clock = LamportClock(node_name)
        self._events: List[messaging_pb2.EventoLamport] = []
        self._events_lock = threading.Lock()

    def _log(self, text: str) -> None:
        if not self.quiet:
            now = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            print(f"[{now}] [{self.node_name}] {text}", flush=True)

    def EnviarMensaje(self, request, context):
        updated_ts = self.clock.update(request.timestamp)
        wall_time = datetime.now(timezone.utc).isoformat()

        if request.record_event:
            evento = messaging_pb2.EventoLamport(
                mechanism="grpc",
                msg_id=request.msg_id,
                node=self.node_name,
                peer=request.sender,
                event_type="recv",
                lamport_clock=updated_ts,
                wall_time_iso=wall_time,
            )
            with self._events_lock:
                self._events.append(evento)

        self._log(
            f"RECV gRPC desde {request.sender} | msg_id={request.msg_id} | "
            f"ts_remoto={request.timestamp} -> ts_local={updated_ts}"
        )
        return messaging_pb2.Acuse(
            status="OK",
            receiver=self.node_name,
            timestamp=updated_ts,
            msg_id=request.msg_id,
        )

    def ReiniciarReloj(self, request, context):
        self.clock.reset(0)
        with self._events_lock:
            self._events.clear()
        return messaging_pb2.ControlResponse(
            status="OK",
            receiver=self.node_name,
            timestamp=0,
        )

    def ObtenerEventos(self, request, context):
        with self._events_lock:
            snapshot = list(self._events)
        return messaging_pb2.ListaEventos(
            receiver=self.node_name,
            events=snapshot,
        )


class GrpcNodeServer:
    def __init__(
        self,
        node_name: str,
        host: Optional[str] = None,
        port: Optional[int] = None,
        quiet: bool = False,
        max_workers: int = 10,
    ) -> None:
        default_host, default_port = get_node_address(node_name)
        self.node_name = node_name
        self.host = host or default_host
        self.port = default_port if port is None else port
        self.quiet = quiet
        self.max_workers = max_workers

        self.servicer = MessagingServicer(node_name, quiet=quiet)
        self._server: Optional[grpc.Server] = None

    def start(self) -> None:
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=self.max_workers))
        messaging_pb2_grpc.add_MessagingServiceServicer_to_server(self.servicer, self._server)
        bound_port = self._server.add_insecure_port(f"{self.host}:{self.port}")
        if bound_port == 0:
            raise RuntimeError(f"No se pudo enlazar el puerto gRPC {self.host}:{self.port}")
        self.port = bound_port
        self._server.start()
        if not self.quiet:
            now = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            print(
                f"[{now}] [{self.node_name}] Servidor gRPC escuchando en {self.host}:{self.port}",
                flush=True,
            )

    def stop(self, grace: float = 0.5) -> None:
        if self._server is not None:
            self._server.stop(grace).wait()

    def serve_forever(self) -> None:
        self.start()
        assert self._server is not None
        try:
            self._server.wait_for_termination()
        except KeyboardInterrupt:
            self.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Servidor gRPC con reloj logico de Lamport.")
    parser.add_argument("--node", required=True, choices=["nodo-A", "nodo-B", "nodo-C"])
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    server = GrpcNodeServer(args.node, host=args.host, port=args.port, quiet=args.quiet)
    server.serve_forever()


if __name__ == "__main__":
    main()
