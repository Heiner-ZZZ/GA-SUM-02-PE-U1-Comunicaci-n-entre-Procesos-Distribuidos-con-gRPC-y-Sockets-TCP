import tempfile
import unittest
from pathlib import Path

import pandas as pd

from analysis.run_analysis import load_latency_csvs, run_analysis
from src.common.config import LATENCY_CSV_FIELDS


def _write_latency_csv(path: Path, mechanism: str, base_ms: float) -> None:
    # CSV con la misma forma que generan los clientes: run 0 de calentamiento + 2 corridas validas
    rows = []
    for mode in ("sequential", "concurrent"):
        for size_label, size_bytes, extra in (("100B", 100, 0.0), ("10KB", 10240, 0.5)):
            for run_id in range(3):
                t0 = 100.0 * run_id
                for seq in range(10):
                    lat = 50.0 if run_id == 0 else base_ms + extra + 0.01 * seq
                    sent = t0 + seq * 0.01
                    rows.append(
                        {
                            "mechanism": mechanism,
                            "mode": mode,
                            "size_label": size_label,
                            "payload_bytes": size_bytes,
                            "run_id": run_id,
                            "is_warmup": run_id == 0,
                            "seq": seq,
                            "sender": "nodo-A",
                            "receiver": "nodo-B",
                            "latency_ms": lat,
                            "sent_perf_s": sent,
                            "recv_perf_s": sent + lat / 1000.0,
                        }
                    )
    pd.DataFrame(rows, columns=LATENCY_CSV_FIELDS).to_csv(path, index=False)


class TestRunAnalysis(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        root = Path(self._tmp.name)
        self.data_dir = root / "data"
        self.out_dir = root / "generated"
        self.fig_dir = root / "figures"
        self.data_dir.mkdir()
        _write_latency_csv(self.data_dir / "latency_sockets.csv", "sockets", 2.0)
        _write_latency_csv(self.data_dir / "latency_grpc.csv", "grpc", 1.0)

    def tearDown(self):
        self._tmp.cleanup()

    def test_generates_tables_and_figures(self):
        result = run_analysis(self.data_dir, self.out_dir, self.fig_dir)

        # 2 mecanismos x 2 modos x 2 tamanos
        summary = result["summary"]
        self.assertEqual(len(summary), 8)
        self.assertEqual(result["rows_total"], 240)
        self.assertEqual(result["rows_valid"], 160)
        # El calentamiento (50 ms) no entra en las metricas
        self.assertTrue((summary["max_ms"] < 50.0).all())

        names = {p.name for p in result["tables"]}
        self.assertEqual(
            names,
            {
                "resumen_latencias.csv",
                "tabla_latencias_sequential.tex",
                "tabla_latencias_concurrent.tex",
                "comparacion_relativa.csv",
                "tabla_comparacion.tex",
            },
        )
        for path in result["tables"] + result["figures"]:
            self.assertTrue(path.exists(), path)
            self.assertGreater(path.stat().st_size, 0, path)

        self.assertEqual(
            {p.name for p in result["figures"]},
            {"boxplot_latencias.png", "barras_latencia_ic95.png", "barras_rendimiento.png"},
        )

        tabla = (self.out_dir / "tabla_latencias_sequential.tex").read_text(encoding="utf-8")
        self.assertIn(r"\toprule", tabla)
        self.assertNotIn("concurrent", tabla)

    def test_comparison_uses_both_mechanisms(self):
        result = run_analysis(self.data_dir, self.out_dir, self.fig_dir)
        comp = result["comparison"]
        self.assertEqual(len(comp), 4)  # 2 modos x 2 tamanos
        # gRPC (1 ms) frente a TCP (2 ms) con 100 B: alrededor de 50 % menos
        row = comp[(comp["mode"] == "sequential") & (comp["size_label"] == "100B")].iloc[0]
        self.assertGreater(row["grpc_mean_reduction_pct"], 40.0)

    def test_missing_data_raises_clear_error(self):
        empty = Path(self._tmp.name) / "vacio"
        empty.mkdir()
        with self.assertRaises(FileNotFoundError):
            load_latency_csvs(empty)


if __name__ == "__main__":
    unittest.main()
