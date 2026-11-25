"""Parse GBN transfer log files and generate efficiency summary tables and charts."""

from __future__ import annotations

import argparse
from pathlib import Path
import re

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

SEND_RE = re.compile(
    r"^\d+,\s*pdu_to_send=(\d+),\s*status=(\w+),\s*ackedNo=(\S+)$"
)
RECV_RE = re.compile(
    r"^\d+,\s*pdu_exp=(\d+),\s*pdu_recv=(\S+),\s*status=(\w+)$"
)


def parse_kv(line: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for part in line.split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            result[k.strip()] = v.strip()
    return result


def parse_log_file(path: Path) -> dict[str, object]:
    meta: dict[str, str] = {}
    summary: dict[str, str] = {}
    send_counts = {"New": 0, "TO": 0, "RT": 0}
    recv_counts = {"OK": 0, "DataErr": 0, "NoErr": 0}

    with path.open("r", encoding="utf-8") as fp:
        for raw_line in fp:
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith("# META:"):
                meta.update(parse_kv(line[len("# META:"):]))
                continue
            if line.startswith("# SUMMARY:"):
                summary.update(parse_kv(line[len("# SUMMARY:"):]))
                continue

            m_send = SEND_RE.match(line)
            if m_send:
                st = m_send.group(2)
                if st in send_counts:
                    send_counts[st] += 1
                continue

            m_recv = RECV_RE.match(line)
            if m_recv:
                st = m_recv.group(3)
                if st in recv_counts:
                    recv_counts[st] += 1

    total_sent = send_counts["New"] + send_counts["TO"] + send_counts["RT"]
    retransmits = send_counts["TO"] + send_counts["RT"]
    elapsed = float(summary.get("elapsed_sec", 0.0))
    bytes_sent = int(summary.get("bytes_sent", 0))
    throughput_kbps = (bytes_sent / 1024.0 / elapsed) if elapsed > 0 else 0.0

    return {
        "file": path.name,
        "HostName": meta.get("HostName", "Unknown"),
        "DataSize": int(meta.get("DataSize", 0)),
        "SWSize": int(meta.get("SWSize", 0)),
        "ErrorRate": float(meta.get("ErrorRate", 0.0)),
        "LostRate": float(meta.get("LostRate", 0.0)),
        "Timeout": int(meta.get("Timeout", 0)),
        "TotalPDUs": send_counts["New"],
        "TotalTransmissions": total_sent,
        "Timeouts": send_counts["TO"],
        "RetransmittedPDUs": retransmits,
        "RecvOK": recv_counts["OK"],
        "RecvDataErr": recv_counts["DataErr"],
        "RecvNoErr": recv_counts["NoErr"],
        "ElapsedSec": round(elapsed, 4),
        "ThroughputKBps": round(throughput_kbps, 2),
    }


def generate_charts(df: pd.DataFrame, out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    saved: list[Path] = []

    # 1. Overview bar chart: Total Transmissions vs Retransmissions per log
    labels = [
        f"{r.HostName}\nD{r.DataSize}/W{r.SWSize}/E{int(r.ErrorRate)}/L{int(r.LostRate)}/T{r.Timeout}"
        for r in df.itertuples()
    ]
    x = range(len(df))

    fig, ax = plt.subplots(figsize=(max(7, len(df) * 1.6), 4.8))
    ax.bar(x, df["TotalPDUs"], width=0.38, label="Original PDUs (New)", color="#2563EB")
    ax.bar(
        [i + 0.38 for i in x],
        df["RetransmittedPDUs"],
        width=0.38,
        label="Retransmitted PDUs (TO + RT)",
        color="#DC2626",
    )
    ax.set_xticks([i + 0.19 for i in x])
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel("PDU Count")
    ax.set_title("GBN Transmission Breakdown: Original vs. Retransmitted PDUs")
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()
    p1 = out_dir / "pdu_transmission_breakdown.png"
    fig.savefig(p1, dpi=150)
    plt.close(fig)
    saved.append(p1)

    # 2. Parameter-sweep charts whenever a parameter varies across logs
    for param, xlabel in [
        ("DataSize", "DataSize (bytes)"),
        ("SWSize", "Sending Window Size (SWSize)"),
        ("ErrorRate", "ErrorRate (%)"),
        ("LostRate", "LostRate (%)"),
        ("Timeout", "Timeout (ms)"),
    ]:
        if df[param].nunique() > 1:
            grouped = df.groupby(param, as_index=False)[
                ["ElapsedSec", "RetransmittedPDUs", "Timeouts", "ThroughputKBps"]
            ].mean()
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4.2))
            ax1.plot(grouped[param], grouped["ElapsedSec"], marker="o", color="#2563EB")
            ax1.set_xlabel(xlabel)
            ax1.set_ylabel("Elapsed Time (s)")
            ax1.set_title(f"Elapsed Time vs {param}")
            ax1.grid(True, linestyle="--", alpha=0.4)

            ax2.plot(
                grouped[param],
                grouped["RetransmittedPDUs"],
                marker="s",
                color="#DC2626",
                label="Retransmits",
            )
            ax2.plot(
                grouped[param],
                grouped["Timeouts"],
                marker="^",
                color="#D97706",
                label="Timeouts",
            )
            ax2.set_xlabel(xlabel)
            ax2.set_ylabel("Count")
            ax2.set_title(f"Retransmissions & Timeouts vs {param}")
            ax2.legend()
            ax2.grid(True, linestyle="--", alpha=0.4)

            fig.tight_layout()
            out_path = out_dir / f"efficiency_vs_{param.lower()}.png"
            fig.savefig(out_path, dpi=150)
            plt.close(fig)
            saved.append(out_path)

    return saved


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze GBN transfer log files")
    parser.add_argument(
        "--log-dir", default="logs", help="Directory containing .log files"
    )
    parser.add_argument(
        "--out-dir", default="analysis_output", help="Directory for CSV and charts"
    )
    args = parser.parse_args()

    log_dir = Path(args.log_dir)
    log_files = sorted(log_dir.glob("*.log"))
    if not log_files:
        print(f"No .log files found in {log_dir}")
        return

    rows = [parse_log_file(p) for p in log_files]
    df = pd.DataFrame(rows)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "summary.csv"
    df.to_csv(csv_path, index=False)

    charts = generate_charts(df, out_dir)
    print(df.to_string(index=False))
    print(f"\nSaved summary table to: {csv_path}")
    for c in charts:
        print(f"Saved chart to        : {c}")


if __name__ == "__main__":
    main()
