import argparse
import socket
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from src.common.config import get_node_address
from src.common.lamport import LamportClock
from src.sockets.framing import recv_json, send_json


class TcpNodeServer:
    def __init__(
        self,
        node_name: str,
        host: Optional[str] = None,
        port: Optional[int] = None,
        quiet: bool = False,
    ) -> None:
        default_host, default_port = get_node_address(node_name)
        self.node_name = node_name
        self.host = host or default_host
        self.port = default_port if port is None else port
        self.quiet = quiet

        self.clock = LamportClock(node_name)
        self._events: List[Dict[str, Any]] = []
        self._events_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._server_sock: Optional[socket.socket] = None
        self._accept_thread: Optional[threading.Thread] = None

    def _log(self, text: str) -> None:
        if not self.quiet:
            now = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            print(f"[{now}] [{self.node_name}] {text}", flush=True)

    def _handle_control(self, data: Dict[str, Any]) -> Dict[str, Any]:
        cmd = data.get("control")
        if cmd == "reset_clock":
            self.clock.reset(0)
            with self._events_lock:
                self._events.clear()
            return {"status": "OK", "receiver": self.node_name, "timestamp": 0}
        if cmd == "get_events":
            with self._events_lock:
                snapshot = list(self._events)
            return {"status": "OK", "receiver": self.node_name, "events": snapshot}
        return {"status": "ERROR", "detail": f"Comando desconocido: {cmd}"}

    def _handle_message(self, data: Dict[str, Any]) -> Dict[str, Any]:
        sender = str(data["sender"])
        received_ts = int(data["timestamp"])
        msg_id = str(data.get("msg_id", ""))
        record_event = bool(data.get("record_event", True))

        updated_ts = self.clock.update(received_ts)
        wall_time = datetime.now(timezone.utc).isoformat()

        if record_event:
            with self._events_lock:
                self._events.append(
                    {
                        "mechanism": "sockets",
                        "msg_id": msg_id,
                        "node": self.node_name,
                        "peer": sender,
                        "event_type": "recv",
                        "lamport_clock": updated_ts,
                        "wall_time_iso": wall_time,
                    }
                )

        self._log(
            f"RECV desde {sender} | msg_id={msg_id} | ts_remoto={received_ts} -> ts_local={updated_ts}"
        )
        return {
            "status": "OK",
            "receiver": self.node_name,
            "timestamp": updated_ts,
            "msg_id": msg_id,
        }

    def _handle_client(self, conn: socket.socket) -> None:
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            while not self._stop_event.is_set():
                data = recv_json(conn)
                if data is None:
                    break
                if "control" in data:
                    reply = self._handle_control(data)
                else:
                    reply = self._handle_message(data)
                send_json(conn, reply)
        except (ConnectionError, OSError, KeyError, ValueError) as exc:
            self._log(f"Conexion cerrada con aviso: {exc}")
        finally:
            conn.close()

    def _accept_loop(self) -> None:
        assert self._server_sock is not None
        while not self._stop_event.is_set():
            try:
                conn, _ = self._server_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            worker = threading.Thread(target=self._handle_client, args=(conn,), daemon=True)
            worker.start()

    def start(self) -> None:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(64)
        srv.settimeout(0.5)
        self.port = srv.getsockname()[1]
        self._server_sock = srv
        self._stop_event.clear()
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()
        self._log(f"Servidor TCP escuchando en {self.host}:{self.port}")

    def stop(self) -> None:
        self._stop_event.set()
        if self._server_sock is not None:
            try:
                self._server_sock.close()
            except OSError:
                pass
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=2.0)

    def serve_forever(self) -> None:
        self.start()
        try:
            while not self._stop_event.is_set():
                self._stop_event.wait(0.5)
        except KeyboardInterrupt:
            self._log("Deteniendo servidor TCP...")
        finally:
            self.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Servidor TCP con reloj logico de Lamport.")
    parser.add_argument("--node", required=True, choices=["nodo-A", "nodo-B", "nodo-C"])
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    server = TcpNodeServer(args.node, host=args.host, port=args.port, quiet=args.quiet)
    server.serve_forever()


if __name__ == "__main__":
    main()
