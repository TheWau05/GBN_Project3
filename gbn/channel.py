"""Unreliable UDP channel wrapper with configurable PDU corruption and loss."""

from __future__ import annotations

import random
import socket


class UnreliableUDPChannel:
    """Wraps a UDP socket to simulate random PDU bit errors and packet drops.

    ErrorRate and LostRate are interpreted as percentages (0..100):
    e.g., ErrorRate=10 means 10% (1 in 10) of outgoing PDUs are corrupted,
    and LostRate=10 means 10% (1 in 10) of outgoing PDUs are dropped.
    """

    def __init__(
        self,
        local_host: str,
        local_port: int,
        error_rate: float = 0.0,
        lost_rate: float = 0.0,
        seed: int | None = None,
    ) -> None:
        self.local_host = local_host
        self.local_port = local_port
        self.error_rate = max(0.0, min(100.0, float(error_rate)))
        self.lost_rate = max(0.0, min(100.0, float(lost_rate)))
        self._rng = random.Random(seed)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Increase OS UDP buffers to reduce unintentional kernel drops on localhost
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4 * 1024 * 1024)
        except OSError:
            pass
        self.sock.bind((local_host, local_port))
        self.sock.settimeout(0.05)

    def send_raw(
        self,
        data: bytes,
        peer_addr: tuple[str, int],
        inject_faults: bool = True,
    ) -> str:
        """Send raw PDU bytes to peer_addr, optionally injecting loss or bit errors.

        Returns one of: 'sent', 'dropped', 'corrupted'.
        """
        if inject_faults:
            if self.lost_rate > 0 and self._rng.random() * 100.0 < self.lost_rate:
                return "dropped"
            if self.error_rate > 0 and self._rng.random() * 100.0 < self.error_rate:
                corrupted = bytearray(data)
                # Flip a bit in the payload or checksum (keep seq readable for logging)
                idx = self._rng.randrange(len(corrupted))
                corrupted[idx] ^= 1 << self._rng.randrange(8)
                self.sock.sendto(bytes(corrupted), peer_addr)
                return "corrupted"

        self.sock.sendto(data, peer_addr)
        return "sent"

    def recv_raw(self, bufsize: int = 8192) -> tuple[bytes, tuple[str, int]] | None:
        """Receive one UDP datagram, returning None on socket timeout."""
        try:
            return self.sock.recvfrom(bufsize)
        except socket.timeout:
            return None

    def close(self) -> None:
        self.sock.close()
