"""
SkyDNS/CoreDNS etcd Service and "s is an X record" inference.

"s is an X record" iff CoreDNS can answer X queries using s.

Service is a Pydantic model aligned with SkyDNS msg.Service (host, port, priority, weight, text, mail, ttl, targetstrip, group, key).
"""

from __future__ import annotations

import ipaddress
from enum import StrEnum

from pydantic import BaseModel, Field


class DnsType(StrEnum):
    """Record type; one enum with literal values."""

    TXT = "TXT"
    MX = "MX"
    SRV = "SRV"
    AAAA = "AAAA"
    A = "A"
    PTR = "PTR"
    CNAME = "CNAME"
    UNKNOWN = "?"


# Sentinel for "port undefined" (CoreDNS excludes from SRV)
SRV_PORT_UNDEFINED = -1


def _non_empty(s: str | None) -> bool:
    """True when not None and not empty string."""
    return s is not None and s != ""


def _parse_ip(host: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    """Parse host as IP; return address or None. Exceptions are absorbed here."""
    if not host:
        return None
    try:
        return ipaddress.ip_address(host)
    except ValueError:
        return None


def _is_ipv4(host: str) -> bool:
    addr = _parse_ip(host)
    return addr is not None and addr.version == 4


def _is_ipv6(host: str) -> bool:
    addr = _parse_ip(host)
    return addr is not None and addr.version == 6


def _is_ip(host: str) -> bool:
    return _parse_ip(host) is not None


# -----------------------------------------------------------------------------
# Service (SkyDNS msg.Service compatible)
# -----------------------------------------------------------------------------


class Service(BaseModel):
    """SkyDNS Service. All JSON fields are Optional (omitempty-style)."""

    host: str | None = Field(
        default=None, description="IP or target hostname (A/AAAA/CNAME/PTR target)"
    )
    port: int | None = Field(default=None, description="SRV port")
    priority: int | None = Field(
        default=None, description="SRV priority; MX preference when mail=True"
    )
    weight: int | None = Field(default=None, description="SRV weight")
    text: str | None = Field(default=None, description="TXT content")
    mail: bool | None = Field(
        default=None, description="If true, treat as MX (Priority → Preference)"
    )
    ttl: int | None = Field(default=None, description="TTL (uint32)")
    targetstrip: int | None = Field(
        default=None, description="Strip N labels from target"
    )
    group: str | None = Field(
        default=None, description="Group services for same answer"
    )
    key: str | None = Field(
        default=None,
        exclude=True,
        description="etcd key where this was read (Go msg.Service.Key, not in JSON)",
    )

    model_config = {
        "populate_by_name": True,
        "extra": "ignore",
        "str_strip_whitespace": True,
    }

    # ----- Spec 8.2: "s is an X record" predicates (methods) -----
    # Reverse-zone check is done by caller; can_be_ptr only checks host is hostname.

    def can_be_a(self) -> bool:
        """s.Host is non-empty and parses as IPv4."""
        return _non_empty(self.host) and _is_ipv4(self.host)

    def can_be_aaaa(self) -> bool:
        """s.Host is non-empty and parses as IPv6."""
        return _non_empty(self.host) and _is_ipv6(self.host)

    def can_be_txt(self) -> bool:
        """s.Text is non-empty and s.Host does not parse as IP."""
        return _non_empty(self.text) and not _is_ip(self.host or "")

    def can_be_cname(self) -> bool:
        """s.Host is non-empty and not an IP (i.e. hostname)."""
        return _non_empty(self.host) and not _is_ip(self.host)

    def can_be_mx(self) -> bool:
        """s.Mail is true and s.Host is non-empty."""
        return self.mail is True and _non_empty(self.host)

    def can_be_srv(self) -> bool:
        """s.Port is defined (not sentinel -1) and s.Host is non-empty."""
        port_defined = self.port is not None and self.port != SRV_PORT_UNDEFINED
        return port_defined and _non_empty(self.host)

    def can_be_ptr(self) -> bool:
        """s.Host is non-empty and not an IP (hostname). Reverse-zone is determined by caller."""
        return _non_empty(self.host) and not _is_ip(self.host)

    def infer_dns_type(self) -> DnsType:
        """Return one representative type when multiple apply. PTR not used here. Priority: TXT > MX > SRV > AAAA > A > CNAME > ?"""
        if self.can_be_txt():
            return DnsType.TXT
        if self.can_be_mx():
            return DnsType.MX
        if self.can_be_srv():
            return DnsType.SRV
        if self.can_be_aaaa():
            return DnsType.AAAA
        if self.can_be_a():
            return DnsType.A
        if self.can_be_cname():
            return DnsType.CNAME
        return DnsType.UNKNOWN


__all__ = [
    "DnsType",
    "Service",
]
