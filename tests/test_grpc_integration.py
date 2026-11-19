import csv
import tempfile
import unittest
from collections import Counter
from pathlib import Path

import pandas as pd

from analysis.metrics import compute_latency_summary, validate_events_df
from src.common.config import EVENT_CSV_FIELDS, LATENCY_CSV_FIELDS, PAYLOAD_SIZES
from src.grpc.client_grpc import (
    EXCHANGE_STEPS,
    NODE_NAMES,
    run_lamport_exchanges,
    run_latency_benchmark,
    save_events_csv,
    save_latency_csv,
)
from src.grpc.server_grpc import GrpcNodeServer

# Parametros reducidos para que la prueba sea rapida; el protocolo completo
# (calentamiento 100 + 5x100) se ejecuta desde la linea de comandos.
TEST_REPETITIONS = 2
TEST_MESSAGES = 6
TEST_WARMUP = 3


class TestGrpcIntegrationThreeNodes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.servers = {}
        for name in NODE_NAMES:
            server = GrpcNodeServer(name, host="127.0.0.1", port=0, quiet=True)
            server.start()
            cls.servers[name] = server
        cls.addresses = {name: ("127.0.0.1", s.port) for name, s in cls.servers.items()}

    @classmethod
    def tearDownClass(cls):
        for server in cls.servers.values():
            server.stop()

    def test_20_lamport_exchanges_over_3_nodes(self):
        events = run_lamport_exchanges(self.addresses)

        self.assertEqual(len(EXCHANGE_STEPS), 18)  # 18 pasos, 2 de ellos con 2 envios simultaneos
        self.assertEqual(len(events), 40)  # 20 envios + 20 recepciones
        self.assertTrue(all(e["mechanism"] == "grpc" for e in events))
        self.assertEqual(Counter(e["event_type"] for e in events), {"send": 20, "recv": 20})

        by_msg = {}
        for e in events:
            by_msg.setdefault(e["msg_id"], {})[e["event_type"]] = e
        self.assertEqual(sorted(by_msg), [f"ex-{i:02d}" for i in range(1, 21)])

        for msg_id, pair in by_msg.items():
            send, recv = pair["send"], pair["recv"]
            # Condicion de reloj de Lamport: send -> recv implica C(send) < C(recv)
            self.assertLess(send["lamport_clock"], recv["lamport_clock"], msg_id)
            self.assertEqual(send["node"], recv["peer"], msg_id)
            self.assertEqual(send["peer"], recv["node"], msg_id)

        # ex-01 y ex-02 se envian a la vez hacia nodo-C: ambas recepciones quedan en nodo-C
        self.assertEqual(by_msg["ex-01"]["recv"]["node"], "nodo-C")
        self.assertEqual(by_msg["ex-02"]["recv"]["node"], "nodo-C")

        # Los relojes de cada nodo receptor crecen estrictamente en sus recepciones
        for node in NODE_NAMES:
            clocks = [e["lamport_clock"] for e in events if e["event_type"] == "recv" and e["node"] == node]
            self.assertEqual(len(clocks), len(set(clocks)), node)

        with tempfile.TemporaryDirectory() as tmp:
            path = save_events_csv(events, Path(tmp) / "events_grpc.csv")
            df = pd.read_csv(path)
            validate_events_df(df)
            self.assertEqual(tuple(df.columns), EVENT_CSV_FIELDS)

    def test_exchanges_are_repeatable_after_reset(self):
        for _ in range(2):
            events = run_lamport_exchanges(self.addresses)
            # ReiniciarReloj limpia los eventos del servidor: no se acumulan entre corridas
            self.assertEqual(len(events), 40)
            # ...y deja los relojes en 0: el primer paso (ex-01 y ex-02 simultaneos desde
            # relojes en 1 hacia nodo-C) siempre produce recepciones con reloj 2 y 3,
            # sin importar cual de los dos llegue primero.
            sends = [e for e in events if e["msg_id"] in ("ex-01", "ex-02") and e["event_type"] == "send"]
            recvs = [e for e in events if e["msg_id"] in ("ex-01", "ex-02") and e["event_type"] == "recv"]
            self.assertEqual(sorted(e["lamport_clock"] for e in sends), [1, 1])
            self.assertEqual(sorted(e["lamport_clock"] for e in recvs), [2, 3])

    def test_benchmark_100b_10kb_sequential_and_concurrent(self):
        rows = run_latency_benchmark(
            self.addresses,
            sizes=PAYLOAD_SIZES,
            repetitions=TEST_REPETITIONS,
            messages_per_run=TEST_MESSAGES,
            warmup_messages=TEST_WARMUP,
        )

        per_client = TEST_WARMUP + TEST_REPETITIONS * TEST_MESSAGES
        expected_per_size = per_client + len(NODE_NAMES) * per_client
        self.assertEqual(len(rows), expected_per_size * len(PAYLOAD_SIZES))

        for size_label, size_bytes in PAYLOAD_SIZES.items():
            size_rows = [r for r in rows if r["size_label"] == size_label]
            self.assertTrue(all(r["payload_bytes"] == size_bytes for r in size_rows))

            seq_rows = [r for r in size_rows if r["mode"] == "sequential"]
            conc_rows = [r for r in size_rows if r["mode"] == "concurrent"]
            self.assertEqual(len(seq_rows), per_client)
            self.assertEqual(len(conc_rows), len(NODE_NAMES) * per_client)

            # Calentamiento solo en run_id 0 y con la cantidad indicada
            warm = [r for r in seq_rows if r["is_warmup"]]
            self.assertEqual(len(warm), TEST_WARMUP)
            self.assertTrue(all(r["run_id"] == 0 for r in warm))
            self.assertEqual(
                sorted({r["run_id"] for r in seq_rows if not r["is_warmup"]}),
                list(range(1, TEST_REPETITIONS + 1)),
            )

            # Secuencial: nodo-A reparte en round-robin entre los 3 nodos
            self.assertEqual({r["receiver"] for r in seq_rows}, set(NODE_NAMES))
            # Concurrente: 3 clientes en paralelo, nunca se envian a si mismos
            self.assertEqual({r["sender"] for r in conc_rows}, set(NODE_NAMES))
            self.assertTrue(all(r["sender"] != r["receiver"] for r in conc_rows))

        for r in rows:
            self.assertEqual(r["mechanism"], "grpc")
            self.assertGreater(r["latency_ms"], 0.0)
            self.assertGreaterEqual(r["recv_perf_s"], r["sent_perf_s"])

        with tempfile.TemporaryDirectory() as tmp:
            path = save_latency_csv(rows, Path(tmp) / "latency_grpc.csv")
            with path.open(encoding="utf-8") as fh:
                header = next(csv.reader(fh))
            self.assertEqual(tuple(header), LATENCY_CSV_FIELDS)

            # El CSV es consumible por el motor estadistico del equipo
            summary = compute_latency_summary(pd.read_csv(path))
            self.assertEqual(set(summary["mechanism"]), {"grpc"})
            self.assertEqual(set(summary["mode"]), {"sequential", "concurrent"})
            self.assertEqual(set(summary["size_label"]), set(PAYLOAD_SIZES))


if __name__ == "__main__":
    unittest.main()
