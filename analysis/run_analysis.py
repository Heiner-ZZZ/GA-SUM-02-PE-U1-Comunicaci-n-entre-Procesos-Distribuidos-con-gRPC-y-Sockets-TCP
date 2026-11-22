import argparse
from pathlib import Path
from typing import Dict, List

import matplotlib

matplotlib.use("Agg")  # sin ventana: solo se guardan los archivos
import matplotlib.pyplot as plt
import pandas as pd

from analysis.metrics import (
    compute_latency_summary,
    compute_relative_comparison,
    filter_valid_runs,
    render_booktabs_latency_table,
    validate_latency_df,
)
from src.common.config import ANALYSIS_DIR, DATA_DIR, DOCS_FIGURES_DIR

GENERATED_DIR = ANALYSIS_DIR / "generated"

MECHANISMS = ("sockets", "grpc")
MECH_LABELS = {"sockets": "Sockets TCP", "grpc": "gRPC"}
MECH_COLORS = {"sockets": "#4C72B0", "grpc": "#DD8452"}
MODES = ("sequential", "concurrent")
MODE_LABELS = {"sequential": "Secuencial", "concurrent": "Concurrente (3 clientes)"}
SIZE_ORDER = ("100B", "10KB")


def load_latency_csvs(data_dir: Path) -> pd.DataFrame:
    files = sorted(data_dir.glob("latency_*.csv"))
    if not files:
        raise FileNotFoundError(
            f"No hay archivos latency_*.csv en {data_dir}. "
            "Ejecute primero los clientes de sockets y gRPC."
        )
    frames = []
    for path in files:
        df = pd.read_csv(path)
        validate_latency_df(df)
        frames.append(df)
        print(f"[analisis] Leido {path.name}: {len(df)} filas")
    return pd.concat(frames, ignore_index=True)


def _sizes_present(df: pd.DataFrame) -> List[str]:
    present = list(df["size_label"].astype(str).unique())
    ordered = [s for s in SIZE_ORDER if s in present]
    return ordered + sorted(s for s in present if s not in SIZE_ORDER)


def _mechanisms_present(df: pd.DataFrame) -> List[str]:
    present = set(df["mechanism"].astype(str))
    return [m for m in MECHANISMS if m in present] + sorted(present - set(MECHANISMS))


def _modes_present(df: pd.DataFrame) -> List[str]:
    present = set(df["mode"].astype(str))
    return [m for m in MODES if m in present]


def render_comparison_table(comparison_df: pd.DataFrame) -> str:
    lines = [
        r"\begin{table}[htbp]",
        r"\centering",
        r"\caption{Variación porcentual de gRPC respecto a Sockets TCP (valores positivos favorecen a gRPC)}",
        r"\label{tab:comparacion_relativa}",
        r"\footnotesize",
        r"\begin{tabular}{llrrrrr}",
        r"\toprule",
        r"Modo & Tamaño & TCP media (ms) & gRPC media (ms) & $\Delta$ media (\%) & $\Delta$ P95 (\%) & $\Delta$ rend. (\%) \\",
        r"\midrule",
    ]
    for _, r in comparison_df.iterrows():
        lines.append(
            f"{MODE_LABELS.get(r['mode'], r['mode'])} & {r['size_label']} & "
            f"{r['tcp_mean_ms']:.4f} & {r['grpc_mean_ms']:.4f} & "
            f"{r['grpc_mean_reduction_pct']:.2f} & {r['grpc_p95_reduction_pct']:.2f} & "
            f"{r['grpc_throughput_gain_pct']:.2f} \\\\"
        )
    lines.extend([r"\bottomrule", r"\end{tabular}", r"\end{table}"])
    return "\n".join(lines) + "\n"


def write_tables(summary: pd.DataFrame, comparison: pd.DataFrame, out_dir: Path) -> List[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    written: List[Path] = []

    path = out_dir / "resumen_latencias.csv"
    summary.to_csv(path, index=False)
    written.append(path)

    # La tabla booktabs de metrics.py no tiene columna de modo, por eso se genera una por modo
    for mode in _modes_present(summary):
        part = summary[summary["mode"] == mode]
        path = out_dir / f"tabla_latencias_{mode}.tex"
        path.write_text(
            render_booktabs_latency_table(
                part,
                caption=f"Latencia RTT y rendimiento por mecanismo y tamaño -- modo {MODE_LABELS[mode].lower()}",
                label=f"tab:latencias_{mode}",
            ),
            encoding="utf-8",
        )
        written.append(path)

    if not comparison.empty:
        path = out_dir / "comparacion_relativa.csv"
        comparison.to_csv(path, index=False)
        written.append(path)
        path = out_dir / "tabla_comparacion.tex"
        path.write_text(render_comparison_table(comparison), encoding="utf-8")
        written.append(path)

    return written


def plot_boxplot(valid: pd.DataFrame, fig_dir: Path) -> Path:
    modes = _modes_present(valid)
    sizes = _sizes_present(valid)
    mechs = _mechanisms_present(valid)

    fig, axes = plt.subplots(1, len(modes), figsize=(6 * len(modes), 4.5), squeeze=False)
    for ax, mode in zip(axes[0], modes):
        data, positions, colors = [], [], []
        for i, size in enumerate(sizes):
            for j, mech in enumerate(mechs):
                sel = valid[
                    (valid["mode"] == mode)
                    & (valid["size_label"] == size)
                    & (valid["mechanism"] == mech)
                ]["latency_ms"]
                if sel.empty:
                    continue
                data.append(sel.to_numpy())
                positions.append(i * (len(mechs) + 1) + j)
                colors.append(MECH_COLORS.get(mech, "gray"))

        box = ax.boxplot(
            data,
            positions=positions,
            widths=0.7,
            patch_artist=True,
            flierprops={"marker": ".", "markersize": 2, "alpha": 0.4},
        )
        for patch, color in zip(box["boxes"], colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.75)
        for median in box["medians"]:
            median.set_color("black")

        centers = [i * (len(mechs) + 1) + (len(mechs) - 1) / 2 for i in range(len(sizes))]
        ax.set_xticks(centers)
        ax.set_xticklabels(sizes)
        ax.set_xlabel("Tamaño del mensaje")
        ax.set_ylabel("Latencia RTT (ms)")
        ax.set_title(MODE_LABELS[mode])
        ax.grid(axis="y", alpha=0.3)

    handles = [plt.Rectangle((0, 0), 1, 1, color=MECH_COLORS.get(m, "gray"), alpha=0.75) for m in mechs]
    fig.legend(handles, [MECH_LABELS.get(m, m) for m in mechs], loc="upper right")
    fig.suptitle("Distribución de la latencia RTT (corridas válidas, sin calentamiento)")
    fig.tight_layout(rect=(0, 0, 0.9, 0.95))

    fig_dir.mkdir(parents=True, exist_ok=True)
    path = fig_dir / "boxplot_latencias.png"
    fig.savefig(path, dpi=300)
    plt.close(fig)
    return path


def _plot_grouped_bars(
    summary: pd.DataFrame,
    value_col: str,
    error_col: str,
    ylabel: str,
    title: str,
    path: Path,
) -> Path:
    modes = _modes_present(summary)
    sizes = _sizes_present(summary)
    mechs = _mechanisms_present(summary)
    width = 0.8 / len(mechs)

    fig, axes = plt.subplots(1, len(modes), figsize=(6 * len(modes), 4.5), squeeze=False)
    for ax, mode in zip(axes[0], modes):
        for j, mech in enumerate(mechs):
            values: List[float] = []
            errors: List[float] = []
            for size in sizes:
                row = summary[
                    (summary["mode"] == mode)
                    & (summary["size_label"] == size)
                    & (summary["mechanism"] == mech)
                ]
                values.append(float(row[value_col].iloc[0]) if not row.empty else 0.0)
                errors.append(float(row[error_col].iloc[0]) if (error_col and not row.empty) else 0.0)
            x = [i + (j - (len(mechs) - 1) / 2) * width for i in range(len(sizes))]
            bars = ax.bar(
                x,
                values,
                width,
                yerr=errors if error_col else None,
                capsize=5,
                color=MECH_COLORS.get(mech, "gray"),
                alpha=0.85,
                label=MECH_LABELS.get(mech, mech),
            )
            ax.bar_label(bars, fmt="%.3f" if value_col == "mean_ms" else "%.0f", fontsize=8, padding=3)

        ax.set_xticks(range(len(sizes)))
        ax.set_xticklabels(sizes)
        ax.set_xlabel("Tamaño del mensaje")
        ax.set_ylabel(ylabel)
        ax.set_title(MODE_LABELS[mode])
        ax.grid(axis="y", alpha=0.3)
        ax.margins(y=0.15)  # espacio para las etiquetas encima de las barras

    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper right")
    fig.suptitle(title)
    fig.tight_layout(rect=(0, 0, 0.9, 0.95))
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=300)
    plt.close(fig)
    return path


def plot_mean_with_ci(summary: pd.DataFrame, fig_dir: Path) -> Path:
    return _plot_grouped_bars(
        summary,
        value_col="mean_ms",
        error_col="ci95_ms",
        ylabel="Latencia media (ms)",
        title="Latencia media con intervalo de confianza del 95 %",
        path=fig_dir / "barras_latencia_ic95.png",
    )


def plot_throughput(summary: pd.DataFrame, fig_dir: Path) -> Path:
    return _plot_grouped_bars(
        summary,
        value_col="throughput_msg_s",
        error_col="",
        ylabel="Rendimiento (mensajes/s)",
        title="Rendimiento promedio por corrida",
        path=fig_dir / "barras_rendimiento.png",
    )


def run_analysis(data_dir: Path, out_dir: Path, fig_dir: Path) -> Dict[str, object]:
    raw = load_latency_csvs(data_dir)
    valid = filter_valid_runs(raw)
    summary = compute_latency_summary(raw)
    comparison = compute_relative_comparison(summary)

    tables = write_tables(summary, comparison, out_dir)
    figures = [
        plot_boxplot(valid, fig_dir),
        plot_mean_with_ci(summary, fig_dir),
        plot_throughput(summary, fig_dir),
    ]
    return {
        "summary": summary,
        "comparison": comparison,
        "tables": tables,
        "figures": figures,
        "rows_total": len(raw),
        "rows_valid": len(valid),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Lee los CSV de latencia de /data/, calcula las metricas y genera tablas y graficos."
    )
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--out-dir", type=Path, default=GENERATED_DIR)
    parser.add_argument("--fig-dir", type=Path, default=DOCS_FIGURES_DIR)
    args = parser.parse_args()

    result = run_analysis(args.data_dir, args.out_dir, args.fig_dir)

    print(
        f"[analisis] {result['rows_total']} filas leidas, "
        f"{result['rows_valid']} validas (sin calentamiento)\n"
    )
    cols = ["mechanism", "mode", "size_label", "runs", "n", "mean_ms", "median_ms",
            "std_ms", "p95_ms", "ci95_ms", "throughput_msg_s"]
    with pd.option_context("display.width", 200, "display.float_format", "{:.4f}".format):
        print(result["summary"][cols].to_string(index=False))
        if not result["comparison"].empty:
            print("\n[analisis] Variacion de gRPC respecto a Sockets TCP (%):")
            print(result["comparison"].to_string(index=False))

    print()
    for path in result["tables"]:
        print(f"[analisis] Tabla   -> {path}")
    for path in result["figures"]:
        print(f"[analisis] Grafico -> {path}")


if __name__ == "__main__":
    main()
