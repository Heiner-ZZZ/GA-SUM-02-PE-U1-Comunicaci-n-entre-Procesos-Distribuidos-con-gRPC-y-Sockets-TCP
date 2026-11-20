import unittest

from src.sockets.client import run_lamport_exchanges, run_latency_benchmark
from src.sockets.server import TcpNodeServer


class TestSocketsIntegration(unittest.TestCase):
    def setUp(self):
        self.servers = {}
        self.addresses = {}
        for name in ("nodo-A", "nodo-B", "nodo-C"):
            srv = TcpNodeServer(name, host="127.0.0.1", port=0, quiet=True)
            srv.start()
            self.servers[name] = srv
            self.addresses[name] = ("127.0.0.1", srv.port)

    def tearDown(self):
        for srv in self.servers.values():
            srv.stop()

    def test_twenty_lamport_exchanges_causal_and_concurrent(self):
        events = run_lamport_exchanges(self.addresses)
        sends = {e["msg_id"]: e for e in events if e["event_type"] == "send"}
        recvs = {e["msg_id"]: e for e in events if e["event_type"] == "recv"}

        self.assertEqual(len(sends), 20)
        self.assertEqual(len(recvs), 20)
        for msg_id, send_ev in sends.items():
            self.assertIn(msg_id, recvs)
            self.assertGreater(int(recvs[msg_id]["lamport_clock"]), int(send_ev["lamport_clock"]))

        # Verifica presencia real de dos eventos concurrentes de nodos distintos con igual reloj
        by_clock = {}
        for ev in events:
            by_clock.setdefault(int(ev["lamport_clock"]), set()).add(ev["node"])
        has_same_clock_pair = any(len(nodes) >= 2 for nodes in by_clock.values())
        self.assertTrue(has_same_clock_pair)

    def test_benchmark_short_long_and_concurrent(self):
        rows = run_latency_benchmark(
            addresses=self.addresses,
            sizes={"100B": 100, "10KB": 10240},
            repetitions=2,
            messages_per_run=5,
            warmup_messages=5,
            include_concurrent=True,
        )
        # Por tamano: seq=(1+2)*5=15, conc=(1+2)*5*3=45 -> 60 por tamano -> 120 filas
        self.assertEqual(len(rows), 120)
        valid = [r for r in rows if not r["is_warmup"]]
        self.assertEqual(len(valid), 80)
        self.assertTrue(all(r["latency_ms"] > 0 for r in valid))
        self.assertEqual({r["mode"] for r in valid}, {"sequential", "concurrent"})
        self.assertEqual({r["size_label"] for r in valid}, {"100B", "10KB"})


if __name__ == "__main__":
    unittest.main()
