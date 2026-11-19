import threading
import unittest

from src.common.config import (
    LONG_PAYLOAD_BYTES,
    NODES,
    SHORT_PAYLOAD_BYTES,
    build_payload,
    get_node_address,
)
from src.common.lamport import LamportClock


class TestLamportClock(unittest.TestCase):
    def test_tick_monotonic(self):
        clk = LamportClock("nodo-A")
        self.assertEqual(clk.value, 0)
        self.assertEqual(clk.tick(), 1)
        self.assertEqual(clk.tick(), 2)

    def test_update_rule(self):
        clk = LamportClock("nodo-B", initial_value=3)
        self.assertEqual(clk.update(10), 11)
        self.assertEqual(clk.update(4), 12)
        self.assertEqual(clk.update(12), 13)

    def test_causal_order(self):
        a = LamportClock("nodo-A")
        b = LamportClock("nodo-B")
        t_send = a.tick()
        t_recv = b.update(t_send)
        self.assertGreater(t_recv, t_send)

    def test_negative_rejected(self):
        with self.assertRaises(ValueError):
            LamportClock("nodo-C", initial_value=-1)
        clk = LamportClock("nodo-C")
        with self.assertRaises(ValueError):
            clk.update(-2)

    def test_concurrent_ticks(self):
        clk = LamportClock("nodo-A")
        threads = [threading.Thread(target=lambda: [clk.tick() for _ in range(200)]) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(clk.value, 1000)


class TestConfig(unittest.TestCase):
    def test_nodes_ports(self):
        self.assertEqual(set(NODES.keys()), {"nodo-A", "nodo-B", "nodo-C"})
        self.assertEqual(get_node_address("nodo-A")[1], 5000)
        self.assertEqual(get_node_address("nodo-B")[1], 5001)
        self.assertEqual(get_node_address("nodo-C")[1], 5002)
        with self.assertRaises(ValueError):
            get_node_address("nodo-X")

    def test_payload_lengths(self):
        p_short = build_payload(SHORT_PAYLOAD_BYTES, prefix="SHORT:")
        p_long = build_payload(LONG_PAYLOAD_BYTES, prefix="LONG:")
        self.assertEqual(len(p_short.encode("utf-8")), 100)
        self.assertEqual(len(p_long.encode("utf-8")), 10240)


if __name__ == "__main__":
    unittest.main()
