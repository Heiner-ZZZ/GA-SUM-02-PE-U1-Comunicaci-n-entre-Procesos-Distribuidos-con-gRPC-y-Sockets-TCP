import math
from typing import Dict, List

import numpy as np
import pandas as pd

from src.common.config import EVENT_CSV_FIELDS, LATENCY_CSV_FIELDS


def _to_bool_series(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes"})


def validate_latency_df(df: pd.DataFrame) -> None:
    missing = [col for col in LATENCY_CSV_FIELDS if col not in df.columns]
    if missing:
        raise ValueError(f"Columnas faltantes en CSV de latencia: {missing}")
    if df.empty:
        raise ValueError("El DataFrame de latencia esta vacio.")


def validate_events_df(df: pd.DataFrame) -> None:
    missing = [col for col in EVENT_CSV_FIELDS if col not in df.columns]
    if missing:
        raise ValueError(f"Columnas faltantes en CSV de eventos: {missing}")
    if df.empty:
        raise ValueError("El DataFrame de eventos esta vacio.")


def filter_valid_runs(df: pd.DataFrame) -> pd.DataFrame:
    validate_latency_df(df)
    warmup_mask = _to_bool_series(df["is_warmup"])
    valid = df.loc[~warmup_mask].copy()
    if valid.empty:
        raise ValueError("No quedan mediciones validas despues de descartar calentamiento.")
    valid["latency_ms"] = pd.to_numeric(valid["latency_ms"], errors="raise")
    valid["sent_perf_s"] = pd.to_numeric(valid["sent_perf_s"], errors="raise")
    valid["recv_perf_s"] = pd.to_numeric(valid["recv_perf_s"], errors="raise")
    return valid


def _compute_group_throughput(group: pd.DataFrame) -> float:
    run_rates: List[float] = []
    for _, run_df in group.groupby("run_id"):
        elapsed = float(run_df["recv_perf_s"].max() - run_df["sent_perf_s"].min())
        if elapsed > 0:
            run_rates.append(len(run_df) / elapsed)
    if not run_rates:
        return 0.0
    return float(np.mean(run_rates))


def compute_latency_summary(df: pd.DataFrame) -> pd.DataFrame:
    valid = filter_valid_runs(df)
    rows: List[Dict[str, object]] = []

    for (mechanism, mode, size_label), grp in valid.groupby(
        ["mechanism", "mode", "size_label"], sort=True
    ):
        lat = grp["latency_ms"].to_numpy(dtype=float)
        n = int(len(lat))
        std_val = float(np.std(lat, ddof=1)) if n > 1 else 0.0
        ci95 = 1.96 * std_val / math.sqrt(n) if n > 1 else 0.0

        rows.append(
            {
                "mechanism": str(mechanism),
                "mode": str(mode),
                "size_label": str(size_label),
                "runs": int(grp["run_id"].nunique()),
                "n": n,
                "mean_ms": float(np.mean(lat)),
                "median_ms": float(np.median(lat)),
                "std_ms": std_val,
                "p95_ms": float(np.percentile(lat, 95)),
                "min_ms": float(np.min(lat)),
                "max_ms": float(np.max(lat)),
                "ci95_ms": ci95,
                "throughput_msg_s": _compute_group_throughput(grp),
            }
        )

    return pd.DataFrame(rows)


def compute_relative_comparison(summary_df: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    for (mode, size_label), grp in summary_df.groupby(["mode", "size_label"], sort=True):
        by_mech = {row["mechanism"]: row for _, row in grp.iterrows()}
        if "sockets" not in by_mech or "grpc" not in by_mech:
            continue
        tcp = by_mech["sockets"]
        grpc_row = by_mech["grpc"]

        mean_diff_pct = (
            ((tcp["mean_ms"] - grpc_row["mean_ms"]) / tcp["mean_ms"]) * 100.0
            if tcp["mean_ms"] > 0
            else 0.0
        )
        p95_diff_pct = (
            ((tcp["p95_ms"] - grpc_row["p95_ms"]) / tcp["p95_ms"]) * 100.0
            if tcp["p95_ms"] > 0
            else 0.0
        )
        thr_diff_pct = (
            ((grpc_row["throughput_msg_s"] - tcp["throughput_msg_s"]) / tcp["throughput_msg_s"])
            * 100.0
            if tcp["throughput_msg_s"] > 0
            else 0.0
        )

        rows.append(
            {
                "mode": str(mode),
                "size_label": str(size_label),
                "tcp_mean_ms": float(tcp["mean_ms"]),
                "grpc_mean_ms": float(grpc_row["mean_ms"]),
                "grpc_mean_reduction_pct": float(mean_diff_pct),
                "grpc_p95_reduction_pct": float(p95_diff_pct),
                "grpc_throughput_gain_pct": float(thr_diff_pct),
            }
        )

    return pd.DataFrame(rows)


def _format_mechanism_label(name: str) -> str:
    mapping = {"sockets": "Sockets TCP", "grpc": "gRPC"}
    return mapping.get(name.lower(), name)


def render_booktabs_latency_table(
    summary_df: pd.DataFrame,
    caption: str,
    label: str,
) -> str:
    lines = [
        r"\begin{table}[htbp]",
        r"\centering",
        rf"\caption{{{caption}}}",
        rf"\label{{{label}}}",
        r"\small",
        r"\begin{tabular}{llrrrrrr}",
        r"\toprule",
        r"Mecanismo & Tamaño & $n$ & Media (ms) & Mediana (ms) & Desv. Est. (ms) & P95 (ms) & Rend. (msg/s) \\",
        r"\midrule",
    ]
    for _, r in summary_df.iterrows():
        mech = _format_mechanism_label(str(r["mechanism"]))
        size = str(r["size_label"])
        lines.append(
            f"{mech} & {size} & {int(r['n'])} & {r['mean_ms']:.4f} & "
            f"{r['median_ms']:.4f} & {r['std_ms']:.4f} & {r['p95_ms']:.4f} & "
            f"{r['throughput_msg_s']:.1f} \\\\"
        )
    lines.extend(
        [
            r"\bottomrule",
            r"\end{tabular}",
            r"\end{table}",
        ]
    )
    return "\n".join(lines) + "\n"
