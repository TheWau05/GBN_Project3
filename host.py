"""CLI entry point to run a single GBN host (Host1 or Host2) from a config file."""

from __future__ import annotations

import argparse
from pathlib import Path

from gbn.protocol import GBNConfig, GBNHost


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reliable File Transfer using Go-Back-N (GBN) over UDP"
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to host INI configuration file (e.g., config_host1.ini)",
    )
    parser.add_argument("--send-file", help="Override SendFile from config")
    parser.add_argument("--recv-file", help="Override RecvFile from config")
    parser.add_argument("--log-file", help="Explicit output log file path")
    args = parser.parse_args()

    cfg = GBNConfig.from_ini(args.config)
    if args.send_file is not None:
        cfg.send_file = args.send_file
    if args.recv_file is not None:
        cfg.recv_file = args.recv_file

    print(
        f"[{cfg.host_name}] UDP {cfg.local_host}:{cfg.udp_port} <-> "
        f"{cfg.peer_host}:{cfg.peer_port} | DataSize={cfg.data_size}B, "
        f"SWSize={cfg.sw_size}, ErrorRate={cfg.error_rate}%, "
        f"LostRate={cfg.lost_rate}%, Timeout={cfg.timeout_ms}ms"
    )
    host = GBNHost(cfg, log_path=Path(args.log_file) if args.log_file else None)
    result = host.run()

    print(f"\n[{cfg.host_name}] Transfer complete!")
    print(f"  Elapsed time   : {result['elapsed_sec']:.3f} s")
    print(f"  Bytes sent     : {result['bytes_sent']:,}")
    print(f"  Bytes received : {result['bytes_received']:,}")
    print(
        f"  Send PDUs      : New={result['pdus_new']}, "
        f"TO={result['pdus_to']}, RT={result['pdus_rt']}"
    )
    print(
        f"  Recv PDUs      : OK={result['recv_ok']}, "
        f"DataErr={result['recv_data_err']}, NoErr={result['recv_no_err']}"
    )
    print(f"  Log saved to   : {result['log_path']}")


if __name__ == "__main__":
    main()
