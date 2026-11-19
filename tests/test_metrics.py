import unittest

import pandas as pd

from analysis.metrics import (
    compute_latency_summary,
    compute_relative_comparison,
    filter_valid_runs,
    render_booktabs_latency_table,
    validate_latency_df,
)


def _make_sample_df() -> pd.DataFrame:
    rows = []
    # Corrida 0: calentamiento (debe excluirse)
    for seq in range(5):
        rows.append(
            {
                "mechanism": "sockets",
                "mode": "sequential",
                "size_label": "100B",
                "payload_bytes": 100,
                "run_id": 0,
                "is_warmup": True,
                "seq": seq,
                "sender": "nodo-A",
                "receiver": "nodo-B",
                "latency_ms": 99.0,
                "sent_perf_s": float(seq),
                "recv_perf_s": float(seq) + 0.099,
            }
        )

    # Corridas validas sockets (media = 2.0 ms) y grpc (media = 1.0 ms)
    for mech, base_lat in (("sockets", 2.0), ("grpc", 1.0)):
        for run_id in (1, 2):
            t0 = 10.0 * run_id
            for seq in range(10):
                sent = t0 + seq * 0.005
                lat_ms = base_lat + (0.1 * seq)
                recv = sent + (lat_ms / 1000.0)
                rows.append(
                    {
                        "mechanism": mech,
                        "mode": "sequential",
                        "size_label": "100B",
                        "payload_bytes": 100,
                        "run_id": run_id,
                        "is_warmup": False,
                        "seq": seq,
                        "sender": "nodo-A",
                        "receiver": "nodo-B",
                        "latency_ms": lat_ms,
                        "sent_perf_s": sent,
                        "recv_perf_s": recv,
                    }
                )
    return pd.DataFrame(rows)


class TestMetrics(unittest.TestCase):
    def test_validate_rejects_missing_columns(self):
        bad_df = pd.DataFrame({"mechanism": ["sockets"]})
        with self.assertRaises(ValueError):
            validate_latency_df(bad_df)

    def test_warmup_rows_are_excluded(self):
        df = _make_sample_df()
        valid = filter_valid_runs(df)
        self.assertEqual(len(valid), 40)
        self.assertFalse((valid["latency_ms"] > 50.0).any())

    def test_summary_and_relative_delta(self):
        summary = compute_latency_summary(_make_sample_df())
        self.assertEqual(len(summary), 2)
        tcp_row = summary[summary["mechanism"] == "sockets"].iloc[0]
        grpc_row = summary[summary["mechanism"] == "grpc"].iloc[0]

        self.assertAlmostEqual(tcp_row["mean_ms"], 2.45, places=4)
        self.assertAlmostEqual(grpc_row["mean_ms"], 1.45, places=4)
        self.assertGreater(tcp_row["throughput_msg_s"], 0.0)

        comp = compute_relative_comparison(summary)
        self.assertEqual(len(comp), 1)
        expected_red = ((2.45 - 1.45) / 2.45) * 100.0
        self.assertAlmostEqual(comp.iloc[0]["grpc_mean_reduction_pct"], expected_red, places=3)

    def test_render_booktabs_table(self):
        summary = compute_latency_summary(_make_sample_df())
        tex = render_booktabs_latency_table(summary, "Resultados de prueba", "tab:prueba")
        self.assertIn(r"\toprule", tex)
        self.assertIn(r"\bottomrule", tex)
        self.assertIn("Sockets TCP", tex)
        self.assertIn("gRPC", tex)


if __name__ == "__main__":
    unittest.main()
