"""
etcd client abstraction and implementation; used to inject mocks in tests.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any, Protocol, runtime_checkable

import etcd3

_log = logging.getLogger(__name__)

__all__ = ["EtcdClientProtocol", "EtcdClientWrapper"]


@runtime_checkable
class EtcdClientProtocol(Protocol):
    """Protocol for etcd client: get, get_by_prefix, put, delete."""

    def get(self, key: bytes, **kwargs: Any) -> tuple[bytes | None, Any]:
        """Get single key. Returns (None, None) if missing, else (value_bytes, meta)."""
        ...

    def get_by_prefix(
        self, key_prefix: bytes, **kwargs: Any
    ) -> Iterator[tuple[bytes, Any]]:
        """Get keys by prefix. Yields (value_bytes, meta) per key."""
        ...

    def put(self, key: bytes, value: bytes, **kwargs: Any) -> Any:
        """Write key."""
        ...

    def delete(self, key: bytes, **kwargs: Any) -> Any:
        """Delete key."""


class EtcdClientWrapper:
    """Wraps etcd3.Etcd3Client to satisfy EtcdClientProtocol."""

    def __init__(self, host: str, port: int, **kwargs: Any) -> None:
        self._client: etcd3.Etcd3Client = etcd3.client(host=host, port=port, **kwargs)

    def get(self, key: bytes, **kwargs: Any) -> tuple[bytes | None, Any]:
        return self._client.get(key, **kwargs)

    def get_by_prefix(
        self, key_prefix: bytes, **kwargs: Any
    ) -> Iterator[tuple[bytes, Any]]:
        return self._client.get_prefix(key_prefix, **kwargs)

    def put(self, key: bytes, value: bytes, **kwargs: Any) -> Any:
        _log.debug("put key=%s", key.decode("utf-8"))
        return self._client.put(key, value, **kwargs)

    def delete(self, key: bytes, **kwargs: Any) -> Any:
        _log.debug("delete key=%s", key.decode("utf-8"))
        return self._client.delete(key, **kwargs)
