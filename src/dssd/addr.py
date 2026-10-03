"""Host:port formatting and parsing that doesn't break on IPv6
addresses, which use bracket notation (``[::1]:9000``) precisely because
a bare ``rsplit(":", 1)`` can't tell an address colon from a port colon.
"""

from __future__ import annotations

import socket


def resolve_addr(addr: str) -> str:
    """Resolves a hostname:port address to a numeric-IP address,
    unchanged if already numeric. Needed for raw UDP sendto (e.g. SWIM's
    single join contact), which - unlike gRPC's channel resolver -
    requires an already-resolved sockaddr.
    """
    host, port = split_addr(addr)
    ip = socket.gethostbyname(host)
    return format_addr(ip, port)


def format_addr(host: str, port: int) -> str:
    if ":" in host:
        return f"[{host}]:{port}"
    return f"{host}:{port}"


def split_addr(addr: str) -> tuple[str, int]:
    if addr.startswith("["):
        host, _, rest = addr[1:].partition("]:")
        return host, int(rest)
    host, port = addr.rsplit(":", 1)
    return host, int(port)


def parse_peers(values: list[str]) -> dict[str, str]:
    """Turns repeated ``--peer id=host:port`` CLI values into an id -> addr map."""
    peers: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"invalid --peer {value!r}, want id=host:port")
        peer_id, addr = value.split("=", 1)
        peers[peer_id] = addr
    return peers


def self_addr(bind_addr: str, port: int) -> str:
    """How this process dials its own gRPC server: the bind host, unless
    that's a wildcard (0.0.0.0 / ::), which isn't dialable."""
    host, _ = split_addr(bind_addr)
    return format_addr("127.0.0.1" if host in ("0.0.0.0", "::", "") else host, port)
