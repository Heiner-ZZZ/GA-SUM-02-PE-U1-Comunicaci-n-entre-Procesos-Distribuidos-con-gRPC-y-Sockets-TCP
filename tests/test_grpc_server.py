import threading
import unittest

import grpc

from src.common.config import LONG_PAYLOAD_BYTES, SHORT_PAYLOAD_BYTES, build_payload
from src.grpc import messaging_pb2, messaging_pb2_grpc
from src.grpc.server_grpc import GrpcNodeServer


class TestGrpcServer(unittest.TestCase):
    def setUp(self):
        self.server = GrpcNodeServer("nodo-B", host="127.0.0.1", port=0, quiet=True)
        self.server.start()
        self.channel = grpc.insecure_channel(f"127.0.0.1:{self.server.port}")
        self.stub = messaging_pb2_grpc.MessagingServiceStub(self.channel)

    def tearDown(self):
        self.channel.close()
        self.server.stop()

    def test_short_and_10kb_rpc_with_lamport(self):
        acuse1 = self.stub.EnviarMensaje(
            messaging_pb2.Mensaje(
                sender="nodo-A",
                timestamp=4,
                message=build_payload(SHORT_PAYLOAD_BYTES, "G1:"),
                msg_id="g-01",
                record_event=True,
            )
        )
        self.assertEqual(acuse1.status, "OK")
        self.assertEqual(acuse1.receiver, "nodo-B")
        self.assertEqual(acuse1.timestamp, 5)

        acuse2 = self.stub.EnviarMensaje(
            messaging_pb2.Mensaje(
                sender="nodo-C",
                timestamp=2,
                message=build_payload(LONG_PAYLOAD_BYTES, "G2:"),
                msg_id="g-02",
                record_event=True,
            )
        )
        self.assertEqual(acuse2.timestamp, 6)

        lista = self.stub.ObtenerEventos(messaging_pb2.ControlRequest(requester="test"))
        self.assertEqual(len(lista.events), 2)
        self.assertEqual(lista.events[0].lamport_clock, 5)
        self.assertEqual(lista.events[1].lamport_clock, 6)

        reset = self.stub.ReiniciarReloj(messaging_pb2.ControlRequest(requester="test"))
        self.assertEqual(reset.timestamp, 0)
        lista_vacia = self.stub.ObtenerEventos(messaging_pb2.ControlRequest(requester="test"))
        self.assertEqual(len(lista_vacia.events), 0)

    def test_concurrent_rpc_calls(self):
        errors = []

        def worker(client_name: str):
            try:
                for i in range(15):
                    resp = self.stub.EnviarMensaje(
                        messaging_pb2.Mensaje(
                            sender=client_name,
                            timestamp=i + 1,
                            message="rpc-ping",
                            msg_id=f"{client_name}-{i}",
                            record_event=False,
                        )
                    )
                    if resp.timestamp <= i + 1:
                        errors.append(f"Timestamp invalido en {client_name}-{i}")
            except Exception as exc:
                errors.append(str(exc))

        threads = [
            threading.Thread(target=worker, args=(n,))
            for n in ("nodo-A", "nodo-B", "nodo-C")
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        self.assertGreaterEqual(self.server.servicer.clock.value, 45)


if __name__ == "__main__":
    unittest.main()
