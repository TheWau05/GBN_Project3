"""CRC-CCITT (polynomial 0x1021, initial value 0xFFFF) implementation."""

from __future__ import annotations

POLY = 0x1021
INIT = 0xFFFF


def _build_table() -> tuple[int, ...]:
    table = []
    for byte in range(256):
        crc = byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ POLY) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
        table.append(crc)
    return tuple(table)


_CRC_TABLE = _build_table()


def crc16_ccitt(data: bytes) -> int:
    """Compute 16-bit CRC-CCITT checksum for the given byte sequence."""
    crc = INIT
    for b in data:
        crc = ((crc << 8) & 0xFFFF) ^ _CRC_TABLE[((crc >> 8) ^ b) & 0xFF]
    return crc
