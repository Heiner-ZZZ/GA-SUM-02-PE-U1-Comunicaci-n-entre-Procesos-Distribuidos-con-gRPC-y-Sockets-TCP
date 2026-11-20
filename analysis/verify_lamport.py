import argparse
import json
import sys
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

from analysis.metrics import validate_events_df
from src.common.config import ANALYSIS_DIR, DATA_DIR


def _mechanism_label(name: str) -> str:
    return {"sockets": "Sockets TCP", "grpc": "gRPC"}.get(name.lower(), name)


def verify_events_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
    validate_events_df(df)
    work = df.copy()
    work["lamport_clock"] = pd.to_numeric(work["lamport_clock"], errors="raise").astype(int)
    work["event_type"] = work["event_type"].astype(str).str.strip().str.lower()

    mechanisms = sorted(work["mechanism"].astype(str).unique().tolist())
    mechanism = mechanisms[0] if len(mechanisms) == 1 else "+".join(mechanisms)

    message_checks: List[Dict[str, Any]] = []
    causal_violations: List[Dict[str, Any]] = []
    pairing_errors: List[str] = []

    for msg_id, grp in work.groupby("msg_id", sort=True):
        sends = grp[grp["event_type"] == "send"]
        recvs = grp[grp["event_type"] == "recv"]
        if len(sends) != 1 or len(recvs) != 1:
            pairing_errors.append(
                f"{msg_id}: se esperaba 1 send y 1 recv (encontrados {len(sends)} send, {len(recvs)} recv)"
            )
            continue

        s = sends.iloc[0]
        r = recvs.iloc[0]
        c_send = int(s["lamport_clock"])
        c_recv = int(r["lamport_clock"])
        nodes_match = str(s["node"]) == str(r["peer"]) and str(s["peer"]) == str(r["node"])
        causal_ok = c_recv > c_send and nodes_match

        entry = {
            "msg_id": str(msg_id),
            "sender": str(s["node"]),
            "receiver": str(r["node"]),
            "send_clock": c_send,
            "recv_clock": c_recv,
            "clock_delta": c_recv - c_send,
            "nodes_match": nodes_match,
            "causal_ok": causal_ok,
        }
        message_checks.append(entry)
        if not causal_ok:
            causal_violations.append(entry)

    local_duplicates: List[Dict[str, Any]] = []
    per_node_summary: Dict[str, Dict[str, int]] = {}
    for node_name, grp in work.groupby("node", sort=True):
        clocks = grp["lamport_clock"].tolist()
        seen: Dict[int, int] = {}
        for val in clocks:
            seen[val] = seen.get(val, 0) + 1
        dups = sorted(k for k, count in seen.items() if count > 1)
        if dups:
            local_duplicates.append({"node": str(node_name), "duplicate_clocks": dups})
        per_node_summary[str(node_name)] = {
            "events": int(len(clocks)),
            "min_clock": int(min(clocks)),
            "max_clock": int(max(clocks)),
        }

    concurrent_pairs = find_concurrent_same_clock_pairs(work)
    all_valid = (
        len(pairing_errors) == 0
        and len(causal_violations) == 0
        and len(local_duplicates) == 0
        and len(message_checks) > 0
    )

    return {
        "mechanism": mechanism,
        "total_events": int(len(work)),
        "total_messages": int(len(message_checks)),
        "causal_ok_count": int(sum(1 for m in message_checks if m["causal_ok"])),
        "causal_violations": causal_violations,
        "pairing_errors": pairing_errors,
        "local_monotonic_ok": len(local_duplicates) == 0,
        "local_duplicates": local_duplicates,
        "max_clock": int(work["lamport_clock"].max()),
        "per_node": per_node_summary,
        "concurrent_same_clock_pairs": concurrent_pairs,
        "all_valid": all_valid,
        "message_checks": message_checks,
    }


def find_concurrent_same_clock_pairs(df: pd.DataFrame) -> List[Dict[str, Any]]:
    pairs: List[Dict[str, Any]] = []
    for clock_val, grp in df.groupby("lamport_clock", sort=True):
        rows = grp.sort_values(["event_type", "node", "msg_id"], ascending=[False, True, True]).to_dict(
            orient="records"
        )
        for a, b in combinations(rows, 2):
            if str(a["node"]) == str(b["node"]):
                continue
            first, second = sorted(
                (a, b), key=lambda item: (str(item["node"]), str(item["msg_id"]))
            )
            pairs.append(
                {
                    "lamport_clock": int(clock_val),
                    "event_a": {
                        "msg_id": str(first["msg_id"]),
                        "node": str(first["node"]),
                        "peer": str(first["peer"]),
                        "event_type": str(first["event_type"]),
                        "wall_time_iso": str(first["wall_time_iso"]),
                    },
                    "event_b": {
                        "msg_id": str(second["msg_id"]),
                        "node": str(second["node"]),
                        "peer": str(second["peer"]),
                        "event_type": str(second["event_type"]),
                        "wall_time_iso": str(second["wall_time_iso"]),
                    },
                    "same_event_type": str(first["event_type"]) == str(second["event_type"]),
                }
            )
    pairs.sort(key=lambda p: (p["lamport_clock"], not p["same_event_type"]))
    return pairs


def verify_events_csv(csv_path: Path) -> Dict[str, Any]:
    if not csv_path.exists():
        raise FileNotFoundError(f"No se encontro el CSV de eventos: {csv_path}")
    df = pd.read_csv(csv_path)
    result = verify_events_dataframe(df)
    result["source_file"] = csv_path.name
    return result


def _format_concurrent_example(pairs: Sequence[Dict[str, Any]]) -> str:
    if not pairs:
        return "Ninguno"
    p = pairs[0]
    ea = p["event_a"]
    eb = p["event_b"]
    return (
        f"\\texttt{{{ea['msg_id']}}} ({ea['node']}) $\\parallel$ "
        f"\\texttt{{{eb['msg_id']}}} ({eb['node']}), $L={p['lamport_clock']}$"
    )


def render_booktabs_lamport_table(
    results: Sequence[Dict[str, Any]],
    caption: str = "Verificación causal de relojes lógicos de Lamport sobre los registros reales.",
    label: str = "tab:verificacion_lamport",
) -> str:
    lines = [
        r"\begin{table}[htbp]",
        r"\centering",
        rf"\caption{{{caption}}}",
        rf"\label{{{label}}}",
        r"\footnotesize",
        r"\begin{tabular}{lrcrcl}",
        r"\toprule",
        r"Mecanismo & Mensajes & $L(\mathrm{recv}) > L(\mathrm{send})$ & $L_{\max}$ & Pares $L_a=L_b$ & Caso concurrente real \\",
        r"\midrule",
    ]
    for res in results:
        mech = _mechanism_label(str(res["mechanism"]))
        ok = int(res["causal_ok_count"])
        total = int(res["total_messages"])
        pct = (ok / total * 100.0) if total > 0 else 0.0
        example = _format_concurrent_example(res["concurrent_same_clock_pairs"])
        lines.append(
            f"{mech} & {total} & {ok}/{total} ({pct:.0f}\\,\\%) & "
            f"{int(res['max_clock'])} & {len(res['concurrent_same_clock_pairs'])} & {example} \\\\"
        )
    lines.extend(
        [
            r"\bottomrule",
            r"\end{tabular}",
            r"\end{table}",
        ]
    )
    return "\n".join(lines) + "\n"


def _print_report(results: Sequence[Dict[str, Any]]) -> None:
    print("=== Verificacion de relojes logicos de Lamport ===")
    for res in results:
        mech = _mechanism_label(str(res["mechanism"]))
        ok = res["causal_ok_count"]
        total = res["total_messages"]
        pct = (ok / total * 100.0) if total > 0 else 0.0
        status = "OK" if res["all_valid"] else "FALLO"
        print(
            f"\n[{mech}] ({res.get('source_file', '-')}) -> {status} | "
            f"eventos={res['total_events']} | L(recv) > L(send): {ok}/{total} ({pct:.1f}%) | "
            f"L_max={res['max_clock']}"
        )
        print(
            f"  Monotonia local por nodo: "
            f"{'OK' if res['local_monotonic_ok'] else res['local_duplicates']}"
        )
        if res["pairing_errors"]:
            print(f"  Errores de emparejamiento: {res['pairing_errors']}")
        if res["causal_violations"]:
            print(f"  Violaciones causales: {res['causal_violations']}")

        pairs = res["concurrent_same_clock_pairs"]
        print(f"  Eventos concurrentes con igual reloj en nodos distintos ({len(pairs)}):")
        for pair in pairs:
            ea = pair["event_a"]
            eb = pair["event_b"]
            print(
                f"    - L={pair['lamport_clock']}: "
                f"{ea['msg_id']} ({ea['node']} -> {ea['peer']}, {ea['event_type']}) || "
                f"{eb['msg_id']} ({eb['node']} -> {eb['peer']}, {eb['event_type']})"
            )

    print(
        "\nNota: Por la condicion de reloj (a -> b => L(a) < L(b)), dos eventos en nodos distintos "
        "con L(a) == L(b) son concurrentes (a || b). Un reloj escalar de Lamport no permite "
        "distinguir causalidad de concurrencia solo comparando valores; para ello se requieren "
        "relojes vectoriales."
    )


def verify_directory(
    data_dir: Path = DATA_DIR,
    generated_dir: Path = ANALYSIS_DIR / "generated",
    files: Optional[Sequence[Path]] = None,
    write_outputs: bool = True,
) -> List[Dict[str, Any]]:
    targets = (
        list(files)
        if files
        else [data_dir / "events_sockets.csv", data_dir / "events_grpc.csv"]
    )
    results = [verify_events_csv(path) for path in targets]
    _print_report(results)

    if write_outputs:
        generated_dir.mkdir(parents=True, exist_ok=True)
        tex_path = generated_dir / "tabla_lamport.tex"
        json_path = generated_dir / "verificacion_lamport.json"
        tex_path.write_text(render_booktabs_lamport_table(results), encoding="utf-8")
        json_path.write_text(
            json.dumps(results, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        print(f"\nTabla LaTeX generada en: {tex_path}")
        print(f"Resumen JSON generado en: {json_path}")

    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verifica la propiedad causal de Lamport y detecta eventos concurrentes reales."
    )
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--generated-dir", type=Path, default=ANALYSIS_DIR / "generated")
    parser.add_argument(
        "--file",
        dest="files",
        action="append",
        type=Path,
        default=None,
        help="Ruta a un CSV de eventos especifico (puede repetirse).",
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="Solo imprime el resultado en consola sin escribir archivos en analysis/generated/.",
    )
    args = parser.parse_args()

    results = verify_directory(
        data_dir=args.data_dir,
        generated_dir=args.generated_dir,
        files=args.files,
        write_outputs=not args.no_write,
    )
    if not all(r["all_valid"] for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
