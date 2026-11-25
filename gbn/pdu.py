"""Binary PDU (frame) definition, serialization, and CRC-CCITT verification.

Frame layout (big-endian, no start/end flags):
  - frame_type  : 1 byte  (DATA=1, ACK=2, FIN=3)
  - flags       : 1 byte  (bit 0 = HAS_ACK for piggybacked ACK)
  - seq         : 2 bytes (unsigned 16-bit sequence number)
  - ack         : 2 bytes (unsigned 16-bit cumulative ACK number)
  - payload_len : 2 bytes (0..4096 bytes)
  - payload     : payload_len bytes
  - checksum    : 2 bytes (CRC-CCITT over header + payload)
"""

from __future__ import annotations

from dataclasses import dataclass
import struct

from .crc import crc16_ccitt

FRAME_DATA = 1
FRAME_ACK = 2
FRAME_FIN = 3

FLAG_HAS_ACK = 0x01

HEADER_FMT = "!BBHHH"
HEADER_SIZE = struct.calcsize(HEADER_FMT)  # 8 bytes
CRC_FMT = "!H"
CRC_SIZE = struct.calcsize(CRC_FMT)        # 2 bytes
MAX_PAYLOAD_SIZE = 4096


@dataclass(frozen=True)
class PDU:
    frame_type: int
    seq: int
    ack: int = 0
    payload: bytes = b""
    flags: int = 0

    def encode(self) -> bytes:
        if len(self.payload) > MAX_PAYLOAD_SIZE:
            raise ValueError(
                f"Payload length {len(self.payload)} exceeds {MAX_PAYLOAD_SIZE} bytes"
            )
        header = struct.pack(
            HEADER_FMT,
            self.frame_type,
            self.flags,
            self.seq & 0xFFFF,
            self.ack & 0xFFFF,
            len(self.payload),
        )
        body = header + self.payload
        checksum = crc16_ccitt(body)
        return body + struct.pack(CRC_FMT, checksum)

    @classmethod
    def decode(cls, raw: bytes) -> tuple["PDU | None", bool]:
        """Decode raw UDP bytes into (PDU, is_valid_crc).

        Returns (None, False) if the packet is too short to parse a header.
        If the CRC check fails, still returns the parsed header fields with
        is_valid_crc=False so the receiver can log the corrupted seq number.
        """
        if len(raw) < HEADER_SIZE + CRC_SIZE:
            return None, False

        body = raw[:-CRC_SIZE]
        (recv_crc,) = struct.unpack(CRC_FMT, raw[-CRC_SIZE:])
        calc_crc = crc16_ccitt(body)
        crc_ok = recv_crc == calc_crc

        frame_type, flags, seq, ack, payload_len = struct.unpack(
            HEADER_FMT, raw[:HEADER_SIZE]
        )
        payload = body[HEADER_SIZE:]
        if len(payload) != payload_len:
            crc_ok = False

        pdu = cls(
            frame_type=frame_type,
            seq=seq,
            ack=ack,
            payload=payload,
            flags=flags,
        )
        return pdu, crc_ok
