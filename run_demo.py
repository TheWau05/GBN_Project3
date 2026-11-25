"""Local end-to-end demo: runs Host1 <-> Host2 full-duplex transfer on >3 MB files."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import threading

from gbn.protocol import GBNConfig, GBNHost


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fp:
        for chunk in iter(lambda: fp.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_test_file(path: Path, size_mb: float = 3.2) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    target_bytes = int(size_mb * 1024 * 1024)
    if path.exists() and path.stat().st_size == target_bytes:
        return
    with path.open("wb") as fp:
        fp.write(os.urandom(target_bytes))


def main() -> None:
    data_dir = Path("test_data")
    h1_send = data_dir / "host1_original_3.2MB.bin"
    h2_send = data_dir / "host2_original_3.2MB.bin"
    h1_recv = data_dir / "host1_received.bin"
    h2_recv = data_dir / "host2_received.bin"

    ensure_test_file(h1_send, size_mb=3.2)
    ensure_test_file(h2_send, size_mb=3.2)

    cfg1 = GBNConfig.from_ini("config_host1.ini")
    cfg2 = GBNConfig.from_ini("config_host2.ini")
    cfg1.send_file, cfg1.recv_file = str(h1_send), str(h1_recv)
    cfg2.send_file, cfg2.recv_file = str(h2_send), str(h2_recv)

    results: dict[str, dict[str, object]] = {}

    def run_host(cfg: GBNConfig, key: str) -> None:
        host = GBNHost(cfg)
        results[key] = host.run(max_duration_sec=180.0)

    print("Starting full-duplex Host1 <-> Host2 transfer (3.2 MB each way)...")
    t1 = threading.Thread(target=run_host, args=(cfg1, "host1"))
    t2 = threading.Thread(target=run_host, args=(cfg2, "host2"))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    h1_ok = sha256_file(h1_send) == sha256_file(h2_recv)
    h2_ok = sha256_file(h2_send) == sha256_file(h1_recv)

    for key in ("host1", "host2"):
        r = results[key]
        print(
            f"\n[{r['host']}] elapsed={r['elapsed_sec']:.2f}s | "
            f"New={r['pdus_new']}, TO={r['pdus_to']}, RT={r['pdus_rt']} | "
            f"OK={r['recv_ok']}, DataErr={r['recv_data_err']}, NoErr={r['recv_no_err']} | "
            f"log={r['log_path']}"
        )

    print(f"\nSHA-256 verification Host1 -> Host2: {'PASS' if h1_ok else 'FAIL'}")
    print(f"SHA-256 verification Host2 -> Host1: {'PASS' if h2_ok else 'FAIL'}")
    if not (h1_ok and h2_ok):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
