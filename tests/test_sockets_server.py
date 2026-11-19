import socket
import threading
import unittest

from src.common.config import LONG_PAYLOAD_BYTES, SHORT_PAYLOAD_BYTES, build_payload
from src.sockets.framing import recv_json, send_json
from src.sockets.server import TcpNodeServer


class TestTcpFramingAndServer(unittest.TestCase):
    def setUp(self):
        self.server = TcpNodeServer("nodo-A", host="127.0.0.1", port=0, quiet=True)
        self.server.start()

    def tearDown(self):
        self.server.stop()

    def _connect(self) -> socket.socket:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", self.server.port))
        return s

    def test_short_and_10kb_messages_update_lamport(self):
        with self._connect() as sock:
            send_json(
                sock,
                {
                    "sender": "nodo-B",
                    "timestamp": 5,
                    "message": build_payload(SHORT_PAYLOAD_BYTES, "T1:"),
                    "msg_id": "m-01",
                },
            )
            ack1 = recv_json(sock)
            self.assertEqual(ack1["status"], "OK")
            self.assertEqual(ack1["receiver"], "nodo-A")
            self.assertEqual(ack1["timestamp"], 6)

            send_json(
                sock,
                {
                    "sender": "nodo-C",
                    "timestamp": 2,
                    "message": build_payload(LONG_PAYLOAD_BYTES, "T2:"),
                    "msg_id": "m-02",
                },
            )
            ack2 = recv_json(sock)
            self.assertEqual(ack2["timestamp"], 7)

            send_json(sock, {"control": "get_events"})
            ev_resp = recv_json(sock)
            self.assertEqual(len(ev_resp["events"]), 2)
            self.assertEqual(ev_resp["events"][0]["lamport_clock"], 6)
            self.assertEqual(ev_resp["events"][1]["lamport_clock"], 7)

    def test_concurrent_clients(self):
        errors = []

        def send_batch(client_name: str):
            try:
                with self._connect() as sock:
                    for i in range(15):
                        send_json(
                            sock,
                            {
                                "sender": client_name,
                                "timestamp": i + 1,
                                "message": "ping",
                                "msg_id": f"{client_name}-{i}",
                                "record_event": False,
                            },
                        )
                        ack = recv_json(sock)
                        if ack is None or ack["timestamp"] <= i + 1:
                            errors.append(f"ACK invalido para {client_name}-{i}")
            except Exception as exc:
                errors.append(str(exc))

        threads = [
            threading.Thread(target=send_batch, args=(name,))
            for name in ("nodo-A", "nodo-B", "nodo-C")
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        self.assertGreaterEqual(self.server.clock.value, 45)


if __name__ == "__main__":
    unittest.main()
