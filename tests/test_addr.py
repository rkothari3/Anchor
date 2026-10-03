import pytest

from dssd.addr import parse_peers, resolve_addr


def test_resolve_addr_resolves_hostname_to_numeric_ip():
    assert resolve_addr("localhost:9000") == "127.0.0.1:9000"


def test_resolve_addr_leaves_numeric_ip_unchanged():
    assert resolve_addr("10.0.0.5:9000") == "10.0.0.5:9000"


def test_parse_peers():
    assert parse_peers(["a=h1:1", "b=h2:2"]) == {"a": "h1:1", "b": "h2:2"}
    with pytest.raises(ValueError):
        parse_peers(["no-equals-sign"])
