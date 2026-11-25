# Reliable File Transfer using Go-Back-N (GBN) over UDP

Full-duplex Host1 <-> Host2 file transfer implementing Go-Back-N (Tanenbaum *Computer Networks*, 5th Ed., Ch. 3, Protocol 5) over UDP sockets in Python 3.

## Project Structure

- `gbn/crc.py` — Pure-Python CRC-CCITT (`0x1021`, init `0xFFFF`) checksum
- `gbn/pdu.py` — Binary PDU framing (`frame_type`, `flags`, `seq`, `ack`, `payload_len`, `payload`, `crc16`), max 4 KB payload
- `gbn/channel.py` — UDP socket wrapper with configurable `ErrorRate` and `LostRate` fault injection
- `gbn/logger.py` — Spec-compliant per-transfer logger (`New`/`TO`/`RT` and `OK`/`DataErr`/`NoErr`)
- `gbn/protocol.py` — Full-duplex GBN sliding-window sender/receiver engine
- `host.py` — CLI entry point to run Host1 or Host2 independently
- `run_demo.py` — One-command local full-duplex 3.2 MB test with SHA-256 verification
- `analyze_logs.py` — Log parser generating `summary.csv` and `matplotlib` efficiency charts
- `.github/workflows/build-windows.yml` — Cloud build workflow producing a Windows 10 `gbn_host.exe`

## Quick Start (macOS)

```bash
# 1. Activate your Python 3 virtual environment
source .venv/bin/activate

# 2. Run the automated 3.2 MB full-duplex demo (Host1 <-> Host2)
python3 run_demo.py

# 3. Parse logs and generate efficiency charts
python3 analyze_logs.py --log-dir logs --out-dir analysis_output
```

## Running Host1 and Host2 in Two Terminals

```bash
# Terminal 1
python3 host.py --config config_host1.ini

# Terminal 2
python3 host.py --config config_host2.ini
```

*(Update `UDPPort` in `config_host1.ini` to `4xxxx` using the last 4 digits of your Student ID.)*

## Building the Windows 10 `.exe` from macOS

Push this folder to a GitHub repository, open the **Actions** tab, and download the `gbn_host-windows-x64` artifact containing `gbn_host.exe`. On Windows 10, run:

```powershell
.\gbn_host.exe --config config_host1.ini
```
