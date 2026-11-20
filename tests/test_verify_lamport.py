import contextlib
import io
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from analysis.verify_lamport import (
    render_booktabs_lamport_table,
    verify_directory,
    verify_events_dataframe,
)


def _sample_events_df() -> pd.DataFrame:
    rows = [
        ("sockets", "ex-01", "nodo-A", "nodo-C", "send", 1, "2026-11-20T00:00:01.001+00:00"),
        ("sockets", "ex-02", "nodo-B", "nodo-C", "send", 1, "2026-11-20T00:00:01.001+00:00"),
        ("sockets", "ex-01", "nodo-C", "nodo-A", "recv", 2, "2026-11-20T00:00:01.002+00:00"),
        ("sockets", "ex-02", "nodo-C", "nodo-B", "recv", 3, "2026-11-20T00:00:01.003+00:00"),
        ("sockets", "ex-03", "nodo-C", "nodo-A", "send", 4, "2026-11-20T00:00:01.004+00:00"),
        ("sockets", "ex-03", "nodo-A", "nodo-C", "recv", 5, "2026-11-20T00:00:01.005+00:00"),
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "mechanism",
            "msg_id",
            "node",
            "peer",
            "event_type",
            "lamport_clock",
            "wall_time_iso",
        ],
    )


class TestVerifyLamport(unittest.TestCase):
    def test_valid_causal_order_and_concurrent_pair(self):
        res = verify_events_dataframe(_sample_events_df())
        self.assertTrue(res["all_valid"])
        self.assertEqual(res["total_messages"], 3)
        self.assertEqual(res["causal_ok_count"], 3)
        self.assertEqual(res["causal_violations"], [])
        self.assertTrue(res["local_monotonic_ok"])

        pairs = res["concurrent_same_clock_pairs"]
        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0]["lamport_clock"], 1)
        self.assertEqual(pairs[0]["event_a"]["msg_id"], "ex-01")
        self.assertEqual(pairs[0]["event_a"]["node"], "nodo-A")
        self.assertEqual(pairs[0]["event_b"]["msg_id"], "ex-02")
        self.assertEqual(pairs[0]["event_b"]["node"], "nodo-B")

    def test_detects_causal_violation(self):
        df = _sample_events_df()
        df.loc[(df["msg_id"] == "ex-03") & (df["event_type"] == "recv"), "lamport_clock"] = 4
        res = verify_events_dataframe(df)
        self.assertFalse(res["all_valid"])
        self.assertEqual(len(res["causal_violations"]), 1)
        self.assertEqual(res["causal_violations"][0]["msg_id"], "ex-03")

    def test_detects_missing_recv_event(self):
        df = _sample_events_df()
        df = df[~((df["msg_id"] == "ex-03") & (df["event_type"] == "recv"))]
        res = verify_events_dataframe(df)
        self.assertFalse(res["all_valid"])
        self.assertEqual(len(res["pairing_errors"]), 1)

    def test_detects_duplicate_clock_on_same_node(self):
        df = _sample_events_df()
        df.loc[(df["msg_id"] == "ex-02") & (df["event_type"] == "recv"), "lamport_clock"] = 2
        res = verify_events_dataframe(df)
        self.assertFalse(res["all_valid"])
        self.assertFalse(res["local_monotonic_ok"])
        self.assertEqual(res["local_duplicates"][0]["node"], "nodo-C")

    def test_verify_directory_and_booktabs_output(self):
        df_tcp = _sample_events_df()
        df_grpc = _sample_events_df()
        df_grpc["mechanism"] = "grpc"

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            data_dir = tmp_path / "data"
            gen_dir = tmp_path / "generated"
            data_dir.mkdir()
            df_tcp.to_csv(data_dir / "events_sockets.csv", index=False)
            df_grpc.to_csv(data_dir / "events_grpc.csv", index=False)

            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                results = verify_directory(data_dir=data_dir, generated_dir=gen_dir)
            self.assertEqual(len(results), 2)
            self.assertTrue(all(r["all_valid"] for r in results))
            self.assertIn("ex-01", buf.getvalue())
            self.assertIn("ex-02", buf.getvalue())

            tex_file = gen_dir / "tabla_lamport.tex"
            json_file = gen_dir / "verificacion_lamport.json"
            self.assertTrue(tex_file.exists())
            self.assertTrue(json_file.exists())

            tex = render_booktabs_lamport_table(results)
            self.assertIn(r"\toprule", tex)
            self.assertIn(r"\bottomrule", tex)
            self.assertIn("Sockets TCP", tex)
            self.assertIn("gRPC", tex)
            self.assertIn("ex-01", tex)
            self.assertIn("ex-02", tex)


if __name__ == "__main__":
    unittest.main()
