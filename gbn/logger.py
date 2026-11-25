"""Spec-compliant transfer logger for Go-Back-N send and receive events.

Send line format:
  1, pdu_to_send=1, status=New, ackedNo=0
Receive line format:
  1, pdu_exp=1, pdu_recv=1, status=OK
"""

from __future__ import annotations

from pathlib import Path
import threading
import time


class TransferLogger:
    """Thread-safe logger writing one log file per transfer."""

    def __init__(self, log_path: Path, metadata: dict[str, object]) -> None:
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._send_seq = 0
        self._recv_seq = 0
        self._start_time = time.perf_counter()
        self._file = self.log_path.open("w", encoding="utf-8")

        meta_str = ", ".join(f"{k}={v}" for k, v in metadata.items())
        self._file.write(f"# META: {meta_str}\n")
        self._file.flush()

    def log_send(self, pdu_to_send: int, status: str, acked_no: int | str) -> None:
        with self._lock:
            self._send_seq += 1
            self._file.write(
                f"{self._send_seq}, pdu_to_send={pdu_to_send}, status={status}, ackedNo={acked_no}\n"
            )

    def log_recv(self, pdu_exp: int, pdu_recv: int | str, status: str) -> None:
        with self._lock:
            self._recv_seq += 1
            self._file.write(
                f"{self._recv_seq}, pdu_exp={pdu_exp}, pdu_recv={pdu_recv}, status={status}\n"
            )

    def close(self, extra_summary: dict[str, object] | None = None) -> float:
        elapsed = time.perf_counter() - self._start_time
        with self._lock:
            summary = {"elapsed_sec": f"{elapsed:.4f}"}
            if extra_summary:
                summary.update({k: str(v) for k, v in extra_summary.items()})
            summary_str = ", ".join(f"{k}={v}" for k, v in summary.items())
            self._file.write(f"# SUMMARY: {summary_str}\n")
            self._file.flush()
            self._file.close()
        return elapsed
