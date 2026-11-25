"""Full-duplex Go-Back-N (GBN) reliable file transfer engine over UDP."""

from __future__ import annotations

from collections import deque
import configparser
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import threading
import time

from .channel import UnreliableUDPChannel
from .logger import TransferLogger
from .pdu import (
    FLAG_HAS_ACK,
    FRAME_ACK,
    FRAME_DATA,
    FRAME_FIN,
    MAX_PAYLOAD_SIZE,
    PDU,
)


@dataclass
class GBNConfig:
    host_name: str = "Host1"
    local_host: str = "127.0.0.1"
    udp_port: int = 41234
    peer_host: str = "127.0.0.1"
    peer_port: int = 41235
    data_size: int = 1024
    error_rate: float = 0.0
    lost_rate: float = 0.0
    sw_size: int = 4
    init_seq_no: int = 1
    timeout_ms: int = 1000
    seq_bits: int = 16
    send_file: str = ""
    recv_file: str = ""
    log_dir: str = "logs"
    seed: int | None = None

    @property
    def seq_mod(self) -> int:
        return 1 << self.seq_bits

    def validate(self) -> None:
        if not (1 <= self.data_size <= MAX_PAYLOAD_SIZE):
            raise ValueError(
                f"DataSize must be between 1 and {MAX_PAYLOAD_SIZE}, got {self.data_size}"
            )
        if not (2 <= self.seq_bits <= 16):
            raise ValueError(f"SeqBits must be between 2 and 16, got {self.seq_bits}")
        max_window = self.seq_mod - 1
        if not (1 <= self.sw_size <= max_window):
            raise ValueError(
                f"SWSize must satisfy 1 <= SWSize <= 2^SeqBits - 1 ({max_window}), got {self.sw_size}"
            )
        if self.timeout_ms <= 0:
            raise ValueError(f"Timeout must be > 0 ms, got {self.timeout_ms}")
        if not (0.0 <= self.error_rate <= 100.0):
            raise ValueError(f"ErrorRate must be in [0, 100], got {self.error_rate}")
        if not (0.0 <= self.lost_rate <= 100.0):
            raise ValueError(f"LostRate must be in [0, 100], got {self.lost_rate}")

    @classmethod
    def from_ini(cls, path: str | Path) -> "GBNConfig":
        parser = configparser.ConfigParser()
        read_ok = parser.read(path, encoding="utf-8")
        if not read_ok:
            raise FileNotFoundError(f"Config file not found: {path}")

        sec = parser["GBN"]
        seed_raw = sec.get("Seed", "").strip()
        cfg = cls(
            host_name=sec.get("HostName", "Host1").strip(),
            local_host=sec.get("LocalHost", "127.0.0.1").strip(),
            udp_port=sec.getint("UDPPort", 41234),
            peer_host=sec.get("PeerHost", "127.0.0.1").strip(),
            peer_port=sec.getint("PeerPort", 41235),
            data_size=sec.getint("DataSize", 1024),
            error_rate=sec.getfloat("ErrorRate", 0.0),
            lost_rate=sec.getfloat("LostRate", 0.0),
            sw_size=sec.getint("SWSize", 4),
            init_seq_no=sec.getint("InitSeqNo", 1),
            timeout_ms=sec.getint("Timeout", 1000),
            seq_bits=sec.getint("SeqBits", 16),
            send_file=sec.get("SendFile", "").strip(),
            recv_file=sec.get("RecvFile", "").strip(),
            log_dir=sec.get("LogDir", "logs").strip(),
            seed=int(seed_raw) if seed_raw else None,
        )
        cfg.validate()
        return cfg


def _between(a: int, b: int, c: int) -> bool:
    """Return True if b is in [a, c) in circular sequence number space."""
    return (a <= b < c) or (c < a <= b) or (b < c < a)


class GBNHost:
    """Full-duplex Go-Back-N host capable of sending and receiving simultaneously."""

    def __init__(self, config: GBNConfig, log_path: Path | None = None) -> None:
        config.validate()
        self.cfg = config
        self.peer_addr = (config.peer_host, config.peer_port)
        self.seq_mod = config.seq_mod
        self.timeout_sec = config.timeout_ms / 1000.0

        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        default_log = Path(config.log_dir) / (
            f"{config.host_name}_D{config.data_size}_W{config.sw_size}"
            f"_E{int(config.error_rate)}_L{int(config.lost_rate)}_T{config.timeout_ms}_{ts}.log"
        )
        self.log_path = Path(log_path) if log_path else default_log

        self.channel = UnreliableUDPChannel(
            local_host=config.local_host,
            local_port=config.udp_port,
            error_rate=config.error_rate,
            lost_rate=config.lost_rate,
            seed=config.seed,
        )
        self.logger = TransferLogger(
            self.log_path,
            metadata={
                "HostName": config.host_name,
                "UDPPort": config.udp_port,
                "PeerPort": config.peer_port,
                "DataSize": config.data_size,
                "SWSize": config.sw_size,
                "ErrorRate": config.error_rate,
                "LostRate": config.lost_rate,
                "InitSeqNo": config.init_seq_no,
                "Timeout": config.timeout_ms,
                "SeqBits": config.seq_bits,
            },
        )

        # Shared state protected by lock
        self._lock = threading.Lock()
        self._stop_event = threading.Event()

        # Sender state
        init_seq = config.init_seq_no % self.seq_mod
        self._base: int = init_seq
        self._next_seq: int = init_seq
        self._last_acked: int | str = "None"
        self._unacked: deque[tuple[int, PDU]] = deque()
        self._timer_start: float | None = None
        self._send_enabled: bool = bool(config.send_file)
        self._send_done: bool = not self._send_enabled
        self._fin_queued: bool = False

        # Receiver state
        self._expected_seq: int = init_seq
        self._last_recv_ok: int | None = None
        self._recv_enabled: bool = bool(config.recv_file)
        self._recv_done: bool = not self._recv_enabled
        self._recv_fp = None
        self._bytes_sent: int = 0
        self._bytes_received: int = 0

        # Stats
        self.stats = {
            "pdus_new": 0,
            "pdus_to": 0,
            "pdus_rt": 0,
            "recv_ok": 0,
            "recv_data_err": 0,
            "recv_no_err": 0,
        }

    def _next_seq_num(self, seq: int) -> int:
        return (seq + 1) % self.seq_mod

    def _send_pdu(self, pdu: PDU, status: str | None = None) -> None:
        """Encode and send a PDU over the unreliable UDP channel."""
        # Piggyback latest cumulative ACK on outgoing DATA/FIN frames if available
        if pdu.frame_type in (FRAME_DATA, FRAME_FIN) and self._last_recv_ok is not None:
            pdu = PDU(
                frame_type=pdu.frame_type,
                seq=pdu.seq,
                ack=self._last_recv_ok,
                payload=pdu.payload,
                flags=pdu.flags | FLAG_HAS_ACK,
            )

        raw = pdu.encode()
        self.channel.send_raw(raw, self.peer_addr, inject_faults=True)

        if status is not None:
            self.logger.log_send(pdu.seq, status, self._last_acked)
            if status == "New":
                self.stats["pdus_new"] += 1
            elif status == "TO":
                self.stats["pdus_to"] += 1
            elif status == "RT":
                self.stats["pdus_rt"] += 1

    def _send_ack(self, ack_no: int) -> None:
        ack_pdu = PDU(frame_type=FRAME_ACK, seq=0, ack=ack_no, flags=FLAG_HAS_ACK)
        self.channel.send_raw(ack_pdu.encode(), self.peer_addr, inject_faults=True)

    def _handle_ack(self, ack_no: int) -> None:
        """Process a cumulative ACK for our GBN sender window."""
        if not self._unacked:
            return
        if _between(self._base, ack_no, self._next_seq):
            while self._unacked:
                seq_i, _ = self._unacked.popleft()
                self._base = self._next_seq_num(seq_i)
                self._last_acked = seq_i
                if seq_i == ack_no:
                    break
            if self._unacked:
                self._timer_start = time.monotonic()
            else:
                self._timer_start = None
                if self._fin_queued:
                    self._send_done = True

    def _handle_incoming_frame(self, raw: bytes) -> None:
        pdu, crc_ok = PDU.decode(raw)
        with self._lock:
            if not crc_ok or pdu is None:
                recv_seq = pdu.seq if pdu is not None else "?"
                # Only log DataErr for data/fin frames (or unknown corrupted frames)
                if pdu is None or pdu.frame_type in (FRAME_DATA, FRAME_FIN):
                    self.logger.log_recv(self._expected_seq, recv_seq, "DataErr")
                    self.stats["recv_data_err"] += 1
                return

            # 1. Process cumulative ACK if present (pure ACK or piggybacked)
            if pdu.frame_type == FRAME_ACK or (pdu.flags & FLAG_HAS_ACK):
                self._handle_ack(pdu.ack)

            if pdu.frame_type == FRAME_ACK:
                return

            # 2. Process DATA or FIN for receiver side
            if not self._recv_enabled:
                return

            if pdu.frame_type == FRAME_FIN:
                if self._recv_done:
                    # Peer retransmitted FIN because our final ACK was lost
                    if self._last_recv_ok is not None:
                        self._send_ack(self._last_recv_ok)
                    return

                if pdu.seq == self._expected_seq:
                    self.logger.log_recv(self._expected_seq, pdu.seq, "OK")
                    self.stats["recv_ok"] += 1
                    self._last_recv_ok = pdu.seq
                    self._expected_seq = self._next_seq_num(self._expected_seq)
                    self._recv_done = True
                    if self._recv_fp is not None:
                        self._recv_fp.flush()
                        self._recv_fp.close()
                        self._recv_fp = None
                    self._send_ack(self._last_recv_ok)
                else:
                    self.logger.log_recv(self._expected_seq, pdu.seq, "NoErr")
                    self.stats["recv_no_err"] += 1
                    if self._last_recv_ok is not None:
                        self._send_ack(self._last_recv_ok)
                return

            if pdu.frame_type == FRAME_DATA:
                if self._recv_done:
                    if self._last_recv_ok is not None:
                        self._send_ack(self._last_recv_ok)
                    return

                if pdu.seq == self._expected_seq:
                    if self._recv_fp is not None:
                        self._recv_fp.write(pdu.payload)
                    self._bytes_received += len(pdu.payload)
                    self.logger.log_recv(self._expected_seq, pdu.seq, "OK")
                    self.stats["recv_ok"] += 1
                    self._last_recv_ok = pdu.seq
                    self._expected_seq = self._next_seq_num(self._expected_seq)
                    self._send_ack(self._last_recv_ok)
                else:
                    self.logger.log_recv(self._expected_seq, pdu.seq, "NoErr")
                    self.stats["recv_no_err"] += 1
                    if self._last_recv_ok is not None:
                        self._send_ack(self._last_recv_ok)

    def _recv_loop(self) -> None:
        while not self._stop_event.is_set():
            item = self.channel.recv_raw()
            if item is None:
                continue
            raw, _addr = item
            self._handle_incoming_frame(raw)

    def _sender_loop(self) -> None:
        if not self._send_enabled:
            return

        send_path = Path(self.cfg.send_file)
        if not send_path.is_file():
            raise FileNotFoundError(f"SendFile not found: {send_path}")

        with send_path.open("rb") as fp:
            eof = False
            while not self._stop_event.is_set():
                with self._lock:
                    if self._send_done:
                        break

                    # Fill available window slots with new DATA / FIN PDUs
                    while len(self._unacked) < self.cfg.sw_size and not self._fin_queued:
                        if not eof:
                            chunk = fp.read(self.cfg.data_size)
                            if chunk:
                                seq = self._next_seq
                                pdu = PDU(frame_type=FRAME_DATA, seq=seq, payload=chunk)
                                self._unacked.append((seq, pdu))
                                self._next_seq = self._next_seq_num(seq)
                                self._bytes_sent += len(chunk)
                                if len(self._unacked) == 1:
                                    self._timer_start = time.monotonic()
                                self._send_pdu(pdu, status="New")
                            else:
                                eof = True
                        if eof and len(self._unacked) < self.cfg.sw_size and not self._fin_queued:
                            seq = self._next_seq
                            fin_pdu = PDU(frame_type=FRAME_FIN, seq=seq, payload=b"")
                            self._unacked.append((seq, fin_pdu))
                            self._next_seq = self._next_seq_num(seq)
                            self._fin_queued = True
                            if len(self._unacked) == 1:
                                self._timer_start = time.monotonic()
                            self._send_pdu(fin_pdu, status="New")

                    # Check Go-Back-N timeout
                    if (
                        self._unacked
                        and self._timer_start is not None
                        and (time.monotonic() - self._timer_start) >= self.timeout_sec
                    ):
                        self._timer_start = time.monotonic()
                        for idx, (_seq_i, pdu_i) in enumerate(self._unacked):
                            status = "TO" if idx == 0 else "RT"
                            self._send_pdu(pdu_i, status=status)

                time.sleep(0.001)

    def run(self, max_duration_sec: float | None = None) -> dict[str, object]:
        """Run full-duplex transfer until both send and receive complete."""
        if self._recv_enabled:
            recv_path = Path(self.cfg.recv_file)
            recv_path.parent.mkdir(parents=True, exist_ok=True)
            self._recv_fp = recv_path.open("wb")

        recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        send_thread = threading.Thread(target=self._sender_loop, daemon=True)
        recv_thread.start()
        send_thread.start()

        start = time.monotonic()
        try:
            while True:
                with self._lock:
                    both_done = self._send_done and self._recv_done
                if both_done:
                    # Linger briefly to reply to any final retransmitted FIN
                    linger = min(1.0, max(0.25, self.timeout_sec * 1.5))
                    time.sleep(linger)
                    break
                if max_duration_sec is not None and (time.monotonic() - start) > max_duration_sec:
                    raise TimeoutError(
                        f"{self.cfg.host_name} exceeded max_duration_sec={max_duration_sec}s"
                    )
                time.sleep(0.01)
        finally:
            self._stop_event.set()
            send_thread.join(timeout=2.0)
            recv_thread.join(timeout=2.0)
            if self._recv_fp is not None:
                self._recv_fp.flush()
                self._recv_fp.close()
                self._recv_fp = None
            self.channel.close()
            elapsed = self.logger.close(
                extra_summary={
                    "bytes_sent": self._bytes_sent,
                    "bytes_received": self._bytes_received,
                    **self.stats,
                }
            )

        return {
            "host": self.cfg.host_name,
            "elapsed_sec": elapsed,
            "bytes_sent": self._bytes_sent,
            "bytes_received": self._bytes_received,
            "log_path": str(self.log_path),
            **self.stats,
        }
