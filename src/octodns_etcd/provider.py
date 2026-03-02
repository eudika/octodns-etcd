"""
OctoDNS provider for etcd (SkyDNS/CoreDNS compatible).
"""

from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any

from etcd3.exceptions import Etcd3Exception
from octodns.provider.base import BaseProvider
from octodns.record import Record
from pydantic import ValidationError

from octodns_etcd.client import EtcdClientProtocol, EtcdClientWrapper
from octodns_etcd.model import DnsType, Service

_SUPPORTED_NON_PTR = {
    DnsType.A,
    DnsType.AAAA,
    DnsType.TXT,
    DnsType.CNAME,
    DnsType.MX,
    DnsType.SRV,
}

if TYPE_CHECKING:
    from octodns.provider.plan import Plan
    from octodns.zone import Zone

__all__ = ["EtcdProvider"]

DEFAULT_TTL = 3600


def _zone_path(zone_name: str, prefix: PurePosixPath) -> PurePosixPath:
    """Return etcd path from zone name. example.com. → /skydns/com/example"""
    return prefix.joinpath(
        *(segment for segment in reversed(zone_name.split(".")) if segment)
    )


def _dn_to_key(fqdn: str, prefix: PurePosixPath) -> str:
    """Return etcd key from FQDN. www.example.com. → /skydns/example/www"""
    path = prefix.joinpath(
        *(segment for segment in reversed(fqdn.split(".")) if segment)
    )
    return str(path)


def _strip_seq(zone_parts: list[str], rest_parts: list[str]) -> list[str]:
    """Strip seq suffix from path parts under zone."""
    if len(zone_parts) >= 2 and zone_parts[0] == "arpa":
        # Reverse zone: fixed length by IP
        zone_ip_len = len(zone_parts) - 2
        if zone_parts[1] == "in-addr":
            # IPv4: 4 * octets (8bit)
            host_ip_len = 4 - zone_ip_len
        elif zone_parts[1] == "ip6":
            # IPv6: 32 * nibbles (4bit)
            host_ip_len = 32 - zone_ip_len
        else:
            raise ValueError(f"invalid arpa zone: {zone_parts!r}")
        return rest_parts[:host_ip_len]
    else:
        # Forward zone: strip trailing digit (seq)
        if rest_parts and rest_parts[-1].isdigit():
            return rest_parts[:-1]
        else:
            return rest_parts


def _extract_record_name(
    key: str, zone_path: PurePosixPath, prefix: PurePosixPath
) -> str:
    """Extract record name from etcd key; strip seq via _strip_seq, join rest in DNS order."""
    zone_parts = zone_path.relative_to(prefix).parts
    key_path = PurePosixPath(key)
    if not key_path.is_relative_to(zone_path):
        raise ValueError(f"key is not under zone_path: {key!r}")
    rest_parts = key_path.relative_to(zone_path).parts

    name_parts = _strip_seq(zone_parts, rest_parts)

    return ".".join(reversed(name_parts))


def _is_reverse_key(key: str, prefix: PurePosixPath) -> bool:
    """Return True if key is for reverse (arpa) zone."""
    rel = PurePosixPath(key).relative_to(prefix)
    return (
        len(rel.parts) >= 2
        and rel.parts[0] == "arpa"
        and rel.parts[1] in ("in-addr", "ip6")
    )


def _ensure_trailing_dot(s: str) -> str:
    """Append trailing . for FQDN if missing."""
    return s if s.endswith(".") else f"{s}."


def _determine_value(dns_type: DnsType, services: list[Service]) -> dict[str, Any]:
    """Determine value / values for DNS record."""
    if dns_type == DnsType.A:
        # [ '1.2.3.4', ... ]
        return {"values": [s.host for s in services]}
    if dns_type == DnsType.AAAA:
        # [ '2001:db8::1', ... ]
        return {"values": [s.host for s in services]}
    if dns_type == DnsType.TXT:
        # [ 'v=spf1 -all', ... ]
        return {"values": [s.text for s in services if s.text]}
    if dns_type == DnsType.CNAME:
        # 'www.example.com.'
        # NOTE: CNAME uses value, not values
        return {"value": _ensure_trailing_dot(services[0].host)}
    if dns_type == DnsType.MX:
        # [ {'preference': 10, 'exchange': 'mail.example.com.'}, ... ]
        return {
            "values": [
                {
                    "preference": s.priority if s.priority is not None else 0,
                    "exchange": _ensure_trailing_dot(s.host),
                }
                for s in services
            ]
        }
    if dns_type == DnsType.SRV:
        # [ {'priority': 0, 'weight': 5, 'port': 8080, 'target': 'srv.example.com.'}, ... ]
        return {
            "values": [
                {
                    "priority": s.priority if s.priority is not None else 0,
                    "weight": s.weight if s.weight is not None else 0,
                    "port": s.port,
                    "target": _ensure_trailing_dot(s.host),
                }
                for s in services
            ]
        }
    if dns_type == DnsType.PTR:
        # [ 'www.example.com.', ... ]
        return {"values": [_ensure_trailing_dot(s.host) for s in services]}

    # unreachable
    raise ValueError(f"unsupported DNS type: {dns_type!r}")


def _record_to_services(record: Record) -> list[Service]:
    """Convert OctoDNS Record to list of Service."""
    common_dict = {
        "name": record.name,
        "ttl": record.ttl,
    }

    if record._type == "A":
        return [Service(host=v, **common_dict) for v in record.values]
    if record._type == "AAAA":
        return [Service(host=v, **common_dict) for v in record.values]
    if record._type == "TXT":
        return [Service(text=v, **common_dict) for v in record.values]
    if record._type == "CNAME":
        return [Service(host=record.value, **common_dict)]
    if record._type == "MX":
        return [
            Service(host=v.exchange, priority=v.preference, **common_dict)
            for v in record.values
        ]
    if record._type == "SRV":
        return [
            Service(
                host=v.target,
                port=v.port,
                priority=v.priority,
                weight=v.weight,
                **common_dict,
            )
            for v in record.values
        ]
    if record._type == "PTR":
        return [Service(host=v, **common_dict) for v in record.values]
    return []


class EtcdProvider(BaseProvider):
    """OctoDNS provider for etcd."""

    SUPPORTS: set[str] = {"A", "AAAA", "TXT", "CNAME", "MX", "SRV", "PTR"}
    SUPPORTS_GEO = False
    SUPPORTS_ROOT_NS = False

    def __init__(
        self,
        id: str,
        *,
        client: EtcdClientProtocol | None = None,
        host: str = "127.0.0.1",
        port: int = 2379,
        prefix: str = "/skydns",
        default_ttl: int = DEFAULT_TTL,
        **kwargs: Any,
    ) -> None:
        self.log = logging.getLogger(f"{self.__class__.__name__}[{id}]")
        super().__init__(id, **kwargs)
        self._client = client or EtcdClientWrapper(host=host, port=port)
        self._prefix = PurePosixPath(prefix)
        self._default_ttl = default_ttl

    def populate(self, zone: Zone, target: bool = False, lenient: bool = False) -> bool:
        zone_path = _zone_path(zone.name, self._prefix)
        zone_key = str(zone_path)
        raw = sorted(
            self._client.get_by_prefix(zone_key.encode("utf-8")),
            key=lambda x: x[1].key,
        )

        # Group by (dns_type, record name)
        groups: dict[tuple[DnsType, str], list[Service]] = defaultdict(list)
        for value, meta in raw:
            try:
                # TODO: more meaningful error handling
                key = meta.key.decode("utf-8")
                name = _extract_record_name(key, zone_path, self._prefix)
                data = json.loads(value.decode("utf-8"))

                service = Service(**data, key=key)
            except (json.JSONDecodeError, ValueError, ValidationError) as e:
                key_safe = getattr(meta, "key", b"")
                key_repr = (
                    key_safe.decode("utf-8", errors="replace")
                    if key_safe
                    else "<no key>"
                )
                self.log.warning("Invalid key or value: %s, %s", key_repr, e)
                continue

            if _is_reverse_key(key, self._prefix):
                # Reverse zone -> PTR only
                if not service.can_be_ptr():
                    continue
                groups[(DnsType.PTR, name)].append(service)
            else:
                # Forward zone -> infer from content (non-PTR)
                dns_type = service.infer_dns_type()
                if dns_type not in _SUPPORTED_NON_PTR:
                    continue
                groups[(dns_type, name)].append(service)

        # Build OctoDNS Record per group
        for (dns_type, name), services in groups.items():
            # NOTE: ttl from first service in group
            ttl = services[0].ttl or self._default_ttl

            value_dict = _determine_value(dns_type, services)
            data: dict[str, Any] = {"type": dns_type.value, "ttl": ttl, **value_dict}
            zone.add_record(Record.new(zone, name, data, source=self, lenient=lenient))

        return len(groups) > 0

    def _apply(self, plan: Plan) -> int:
        applied = 0
        for change in plan.changes:
            if change.existing is not None:
                applied += self._delete_existing_records(change.existing)
            if change.new is not None:
                applied += self._create_new_records(change.new)
        return applied

    def _delete_existing_records(self, existing: Record) -> int:
        base_key = _dn_to_key(existing.fqdn, self._prefix)
        raw = self._client.get_by_prefix(base_key.encode("utf-8"))
        applied = 0
        for _, meta in raw:
            try:
                self._client.delete(meta.key)
                applied += 1
            except Etcd3Exception as e:
                self.log.warning(
                    "Failed to delete existing record: %s, %s",
                    meta.key.decode("utf-8"),
                    e,
                )
                continue
        return applied

    def _create_new_records(self, new: Record) -> int:
        if new._type not in self.SUPPORTS:
            self.log.warning("Unsupported record type: %s", new._type)
            return 0
        base_key = _dn_to_key(new.fqdn, self._prefix)
        self._migrate_bare_key_to_zero(base_key)
        next_idx = self._next_seq_for_name(base_key)
        services = _record_to_services(new)
        applied = 0
        use_seq = len(services) > 1 or next_idx > 0
        for i, service in enumerate(services):
            key = f"{base_key}/{next_idx + i}" if use_seq else base_key
            value = json.dumps(service.model_dump(exclude_none=True)).encode("utf-8")
            try:
                self._client.put(key.encode("utf-8"), value)
                applied += 1
            except Etcd3Exception as e:
                self.log.warning("Failed to create new record: %s, %s", key, e)
        return applied

    def _migrate_bare_key_to_zero(self, base_key: str) -> None:
        """If bare base_key exists, move its value to base_key/0 and delete the bare key."""
        base_key_bytes = base_key.encode("utf-8")
        value, _ = self._client.get(base_key_bytes)
        if value is not None:
            self._client.put(base_key_bytes + b"/0", value)
            self._client.delete(base_key_bytes)

    def _next_seq_for_name(self, base_key: str) -> int:
        """Return next index for appending under base_key; bare key counts as 0, base_key/N as N."""
        used: set[int] = set()
        for _, meta in self._client.get_by_prefix(base_key.encode("utf-8")):
            key = meta.key.decode("utf-8")
            if key == base_key:
                used.add(0)
            elif (m := re.match(re.escape(base_key) + r"/(\d+)", key)):
                used.add(int(m.group(1)))
        return max(used) + 1 if used else 0
