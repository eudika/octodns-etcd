"""Pytest fixtures for octodns_etcd tests."""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import pytest
from octodns.zone import Zone

from octodns_etcd.client import EtcdClientProtocol


@pytest.fixture
def zone() -> Zone:
    """Default zone for provider tests."""
    return Zone("example.com.", [])


class EtcdKvMeta:
    """Metadata for get_by_prefix results."""

    __slots__ = ("key",)

    def __init__(self, key: str | bytes) -> None:
        self.key = key.encode("utf-8") if isinstance(key, str) else key


class DictEtcdClient(EtcdClientProtocol):
    """EtcdClientProtocol backed by a dict: key (str) -> value (skydns-style dict)."""

    def __init__(self, data: dict[str, dict[str, Any]] | None = None) -> None:
        """data: key -> value dict; value is skydns-style JSON dict."""
        self.data: dict[str, dict[str, Any]] = dict(data) if data else {}

    def get(self, key: bytes, **kwargs: Any) -> tuple[bytes | None, EtcdKvMeta | None]:
        key_str = key.decode("utf-8")
        if key_str not in self.data:
            return (None, None)
        val = self.data[key_str]
        return (json.dumps(val).encode("utf-8"), EtcdKvMeta(key=key_str))

    def get_by_prefix(
        self, key_prefix: bytes, **kwargs: Any
    ) -> Iterator[tuple[bytes, EtcdKvMeta]]:
        prefix_str = key_prefix.decode("utf-8").rstrip("/")
        for key, val in list(self.data.items()):
            if key == prefix_str or key.startswith(prefix_str + "/"):
                yield (json.dumps(val).encode("utf-8"), EtcdKvMeta(key=key))

    def put(self, key: bytes, value: bytes, **kwargs: Any) -> Any:
        self.data[key.decode("utf-8")] = json.loads(value.decode("utf-8"))

    def delete(self, key: bytes, **kwargs: Any) -> Any:
        self.data.pop(key.decode("utf-8"), None)


def make_etcd_kv(key: str, value: dict) -> tuple[bytes, EtcdKvMeta]:
    """Build (value_bytes, meta) for get_by_prefix return value."""
    value_bytes = json.dumps(value).encode("utf-8")
    return (value_bytes, EtcdKvMeta(key=key))
