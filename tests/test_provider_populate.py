"""Tests for octodns_etcd EtcdProvider."""

from __future__ import annotations

import pytest
from octodns.zone import Zone

from octodns_etcd.provider import EtcdProvider, _extract_record_name
from tests.conftest import DictEtcdClient, EtcdKvMeta

PREFIX = "/skydns"
ONE_TO_THIRTY = "1/2/3/4/5/6/7/8/9/a/b/c/d/e/f/0/1/2/3/4/5/6/7/8/9/a/b/c/d/e"


class TestExtractRecordName:
    """Unit tests for _extract_record_name (forward and reverse zones)."""

    @pytest.mark.parametrize(
        ["zone_path_str", "key", "expected"],
        [
            # Forward
            ("/skydns/com/example", "/skydns/com/example", ""),
            ("/skydns/com/example", "/skydns/com/example/0", ""),
            ("/skydns/com/example", "/skydns/com/example/app", "app"),
            ("/skydns/com/example", "/skydns/com/example/app/1", "app"),
            ("/skydns/com/example", "/skydns/com/example/sub/app", "app.sub"),
            ("/skydns/com/example", "/skydns/com/example/sub/app/2", "app.sub"),
            # Reverse IPv4
            ("/skydns/arpa/in-addr/1/2/3/4", "/skydns/arpa/in-addr/1/2/3/4", ""),
            ("/skydns/arpa/in-addr/1/2/3/4", "/skydns/arpa/in-addr/1/2/3/4/0", ""),
            ("/skydns/arpa/in-addr/1/2/3", "/skydns/arpa/in-addr/1/2/3/4", "4"),
            ("/skydns/arpa/in-addr/1/2/3", "/skydns/arpa/in-addr/1/2/3/4/1", "4"),
            ("/skydns/arpa/in-addr/1/2", "/skydns/arpa/in-addr/1/2/3/4", "4.3"),
            ("/skydns/arpa/in-addr/1/2", "/skydns/arpa/in-addr/1/2/3/4/2", "4.3"),
            # Reverse IPv6
            (
                f"/skydns/arpa/ip6/{ONE_TO_THIRTY}/f/0",
                f"/skydns/arpa/ip6/{ONE_TO_THIRTY}/f/0",
                "",
            ),
            (
                f"/skydns/arpa/ip6/{ONE_TO_THIRTY}/f/0",
                f"/skydns/arpa/ip6/{ONE_TO_THIRTY}/f/0/0",
                "",
            ),
            (
                f"/skydns/arpa/ip6/{ONE_TO_THIRTY}/f",
                f"/skydns/arpa/ip6/{ONE_TO_THIRTY}/f/0",
                "0",
            ),
            (
                f"/skydns/arpa/ip6/{ONE_TO_THIRTY}/f",
                f"/skydns/arpa/ip6/{ONE_TO_THIRTY}/f/0/1",
                "0",
            ),
            (
                f"/skydns/arpa/ip6/{ONE_TO_THIRTY}",
                f"/skydns/arpa/ip6/{ONE_TO_THIRTY}/f/0",
                "0.f",
            ),
            (
                f"/skydns/arpa/ip6/{ONE_TO_THIRTY}",
                f"/skydns/arpa/ip6/{ONE_TO_THIRTY}/f/0/2",
                "0.f",
            ),
        ],
    )
    def test_key_equals_zone_path_returns_empty(
        self, zone_path_str, key, expected
    ) -> None:
        assert _extract_record_name(key, zone_path_str, PREFIX) == expected


class TestEtcdProvider:
    """Basic EtcdProvider tests."""

    def test_provider_instantiate(self) -> None:
        provider = EtcdProvider("etcd")
        assert provider.id == "etcd"


class TestEtcdProviderPopulate:
    """populate tests (with client injection)."""

    def test_populate_empty_returns_false(self, zone: Zone) -> None:
        client = DictEtcdClient({})

        provider = EtcdProvider("etcd", client=client)
        exists = provider.populate(zone, target=False, lenient=False)

        assert exists is False
        assert len(zone.records) == 0

    def test_populate_with_keys_returns_true(self, zone: Zone) -> None:
        client = DictEtcdClient(
            {"/skydns/com/example/app": {"host": "192.168.1.10", "ttl": 300}}
        )

        provider = EtcdProvider("etcd", client=client)
        exists = provider.populate(zone, target=False, lenient=False)

        assert exists is True

    def test_populate_uses_custom_path_prefix(self, zone: Zone) -> None:
        """With prefix=/mydns, only data under that prefix is read."""
        client = DictEtcdClient(
            {"/mydns/com/example/app": {"host": "192.168.1.10", "ttl": 300}}
        )

        provider = EtcdProvider("etcd", client=client, prefix="/mydns")
        provider.populate(zone, target=False, lenient=False)

        assert len(zone.records) == 1
        assert list(zone.records)[0].name == "app"
        assert list(list(zone.records)[0].values) == ["192.168.1.10"]


class TestEtcdProviderPopulateARecord:
    """populate: A records from etcd into zone."""

    def test_populate_single_a_record(self, zone: Zone) -> None:
        client = DictEtcdClient(
            {"/skydns/com/example/app": {"host": "192.168.1.10", "ttl": 300}}
        )

        provider = EtcdProvider("etcd", client=client)
        provider.populate(zone, target=False, lenient=False)

        assert len(zone.records) == 1
        rec = list(zone.records)[0]
        assert rec.name == "app"
        assert rec._type == "A"
        assert list(rec.values) == ["192.168.1.10"]
        assert rec.ttl == 300

    def test_populate_only_records_in_zone(self) -> None:
        """Only records in the populated zone are included (e.g. example1 vs example2)."""
        zone = Zone("example1.com.", [])
        client = DictEtcdClient(
            {
                "/skydns/com/example1/app": {"host": "192.168.1.10", "ttl": 300},
                "/skydns/com/example2/app": {"host": "192.168.1.20", "ttl": 300},
            }
        )

        provider = EtcdProvider("etcd", client=client)
        provider.populate(zone, target=False, lenient=False)

        assert len(zone.records) == 1
        rec = list(zone.records)[0]
        assert rec.name == "app"
        assert list(rec.values) == ["192.168.1.10"]

    def test_populate_returns_false_when_no_records_match_any_type(
        self, zone: Zone
    ) -> None:
        """exists False when get_by_prefix returns data that matches no record type."""
        client = DictEtcdClient(
            {"/skydns/com/example/app": {"ttl": 300}}
        )  # no host/text etc. -> no type match

        provider = EtcdProvider("etcd", client=client)
        exists = provider.populate(zone, target=False, lenient=False)

        assert exists is False
        assert len(zone.records) == 0

    def test_populate_a_and_cname_both_added(self, zone: Zone) -> None:
        """When one A and one CNAME are returned, both are added to zone."""
        client = DictEtcdClient(
            {
                "/skydns/com/example/app": {"host": "192.168.1.10", "ttl": 300},
                "/skydns/com/example/www": {"host": "example.com"},
            }
        )

        provider = EtcdProvider("etcd", client=client)
        provider.populate(zone, target=False, lenient=False)

        assert len(zone.records) == 2
        by_name = {rec.name: rec for rec in zone.records}
        assert by_name["app"]._type == "A"
        assert list(by_name["app"].values) == ["192.168.1.10"]
        assert by_name["www"]._type == "CNAME"
        assert by_name["www"].value == "example.com."

    def test_populate_skips_invalid_json(self, zone: Zone) -> None:
        """When one entry is valid JSON and one invalid, only the valid one is added to zone."""

        class ClientWithInvalidEntry(DictEtcdClient):
            def get_by_prefix(self, key_prefix, **kwargs):
                yield from super().get_by_prefix(key_prefix, **kwargs)
                if key_prefix.rstrip(b"/") == b"/skydns/com/example":
                    yield (b"not json", EtcdKvMeta("/skydns/com/example/bad"))

        client = ClientWithInvalidEntry(
            {"/skydns/com/example/app": {"host": "192.168.1.10", "ttl": 300}}
        )

        provider = EtcdProvider("etcd", client=client)
        provider.populate(zone, target=False, lenient=False)

        assert len(zone.records) == 1
        rec = list(zone.records)[0]
        assert rec.name == "app"

    def test_populate_uses_default_ttl_when_ttl_omitted(self, zone: Zone) -> None:
        """When value has no ttl, record ttl is provider default (3600 when not set)."""
        from octodns_etcd.provider import DEFAULT_TTL

        client = DictEtcdClient({"/skydns/com/example/app": {"host": "192.168.1.10"}})

        provider = EtcdProvider("etcd", client=client)
        provider.populate(zone, target=False, lenient=False)

        assert len(zone.records) == 1
        rec = list(zone.records)[0]
        assert rec.ttl == DEFAULT_TTL

    def test_populate_uses_provider_default_ttl_when_ttl_omitted(
        self, zone: Zone
    ) -> None:
        """When value has no ttl, record ttl is provider default_ttl."""
        client = DictEtcdClient({"/skydns/com/example/app": {"host": "192.168.1.10"}})

        provider = EtcdProvider("etcd", client=client, default_ttl=120)
        provider.populate(zone, target=False, lenient=False)

        assert len(zone.records) == 1
        rec = list(zone.records)[0]
        assert rec.ttl == 120

    def test_populate_record_name_from_key_with_trailing_digit_segment(
        self, zone: Zone
    ) -> None:
        """Key like zone_path/app/0 yields record name app."""
        client = DictEtcdClient(
            {"/skydns/com/example/app/0": {"host": "192.168.1.10", "ttl": 300}}
        )

        provider = EtcdProvider("etcd", client=client)
        provider.populate(zone, target=False, lenient=False)

        assert len(zone.records) == 1
        rec = list(zone.records)[0]
        assert rec.name == "app"

    def test_populate_same_name_multiple_a_merged_into_one_record(
        self, zone: Zone
    ) -> None:
        """Multiple keys (app/0, app/1) with same name and both A merge into one record with multiple values."""
        client = DictEtcdClient(
            {
                "/skydns/com/example/app/0": {"host": "192.168.1.10", "ttl": 300},
                "/skydns/com/example/app/1": {"host": "192.168.1.20", "ttl": 300},
            }
        )

        provider = EtcdProvider("etcd", client=client)
        provider.populate(zone, target=False, lenient=False)

        assert len(zone.records) == 1
        rec = list(zone.records)[0]
        assert rec.name == "app"
        assert set(rec.values) == {"192.168.1.10", "192.168.1.20"}

    def test_populate_merged_record_uses_first_ttl(self, zone: Zone) -> None:
        """When merging multiple records, the first ttl is used."""
        client = DictEtcdClient(
            {
                "/skydns/com/example/app/0": {"host": "192.168.1.10", "ttl": 300},
                "/skydns/com/example/app/1": {"host": "192.168.1.20", "ttl": 600},
            }
        )

        provider = EtcdProvider("etcd", client=client)
        provider.populate(zone, target=False, lenient=False)

        assert len(zone.records) == 1
        rec = list(zone.records)[0]
        assert rec.ttl == 300

    def test_populate_multiple_names_multiple_a_records(self, zone: Zone) -> None:
        """When multiple A records with different names exist in the same zone, all are added."""
        client = DictEtcdClient(
            {
                "/skydns/com/example/app": {"host": "192.168.1.10", "ttl": 300},
                "/skydns/com/example/www": {"host": "192.168.1.20", "ttl": 300},
            }
        )

        provider = EtcdProvider("etcd", client=client)
        provider.populate(zone, target=False, lenient=False)

        assert len(zone.records) == 2
        names = {rec.name for rec in zone.records}
        assert names == {"app", "www"}
        by_name = {rec.name: list(rec.values) for rec in zone.records}
        assert by_name["app"] == ["192.168.1.10"]
        assert by_name["www"] == ["192.168.1.20"]


class TestEtcdProviderPopulateOtherTypes:
    """populate: record types other than A (AAAA, TXT, CNAME, MX, SRV)."""

    def test_populate_aaaa_record(self, zone: Zone) -> None:
        client = DictEtcdClient(
            {"/skydns/com/example/app": {"host": "2001:db8::1", "ttl": 300}}
        )
        provider = EtcdProvider("etcd", client=client)
        provider.populate(zone, target=False, lenient=False)
        assert len(zone.records) == 1
        rec = list(zone.records)[0]
        assert rec._type == "AAAA"
        assert list(rec.values) == ["2001:db8::1"]

    def test_populate_txt_record(self, zone: Zone) -> None:
        client = DictEtcdClient(
            {"/skydns/com/example/app": {"text": "v=spf1 -all", "ttl": 300}}
        )
        provider = EtcdProvider("etcd", client=client)
        provider.populate(zone, target=False, lenient=False)
        assert len(zone.records) == 1
        rec = list(zone.records)[0]
        assert rec._type == "TXT"
        assert list(rec.values) == ["v=spf1 -all"]

    def test_populate_mx_record(self, zone: Zone) -> None:
        client = DictEtcdClient(
            {
                "/skydns/com/example/app": {
                    "host": "mail.example.com",
                    "mail": True,
                    "priority": 10,
                    "ttl": 300,
                },
            }
        )
        provider = EtcdProvider("etcd", client=client)
        provider.populate(zone, target=False, lenient=False)
        assert len(zone.records) == 1
        rec = list(zone.records)[0]
        assert rec._type == "MX"
        assert rec.values[0].preference == 10
        assert rec.values[0].exchange == "mail.example.com."

    def test_populate_srv_record(self, zone: Zone) -> None:
        client = DictEtcdClient(
            {
                "/skydns/com/example/_http._tcp": {
                    "host": "srv.example.com",
                    "port": 8080,
                    "priority": 0,
                    "weight": 5,
                    "ttl": 300,
                },
            }
        )
        provider = EtcdProvider("etcd", client=client)
        provider.populate(zone, target=False, lenient=False)
        assert len(zone.records) == 1
        rec = list(zone.records)[0]
        assert rec._type == "SRV"
        assert rec.values[0].priority == 0
        assert rec.values[0].weight == 5
        assert rec.values[0].port == 8080
        assert rec.values[0].target == "srv.example.com."

    def test_populate_www_in_addr_arpa_example_com_a_record(self) -> None:
        """Get A record for www.in-addr.arpa.example.com."""
        zone = Zone("in-addr.arpa.example.com.", [])
        client = DictEtcdClient(
            {
                "/skydns/com/example/arpa/in-addr/www": {
                    "host": "192.168.1.1",
                    "ttl": 300,
                }
            }
        )
        provider = EtcdProvider("etcd", client=client)
        provider.populate(zone, target=False, lenient=False)
        assert len(zone.records) == 1
        rec = list(zone.records)[0]
        assert rec.name == "www"
        assert rec._type == "A"
        assert list(rec.values) == ["192.168.1.1"]

    def test_populate_ptr_zone_1_0_0_127_key_exact(self) -> None:
        """Zone 1.0.0.127.in-addr.arpa., key arpa/in-addr/127/0/0/1; no remainder after zone -> name ""."""
        zone = Zone("1.0.0.127.in-addr.arpa.", [])
        client = DictEtcdClient(
            {"/skydns/arpa/in-addr/127/0/0/1": {"host": "ptr.example.com", "ttl": 60}}
        )
        provider = EtcdProvider("etcd", client=client)
        provider.populate(zone, target=False, lenient=False)
        assert len(zone.records) == 1
        rec = list(zone.records)[0]
        assert rec.name == ""
        assert rec._type == "PTR"
        assert list(rec.values) == ["ptr.example.com."]

    def test_populate_ptr_zone_1_0_0_127_key_with_seq(self) -> None:
        """Zone 1.0.0.127.in-addr.arpa., key arpa/in-addr/127/0/0/1/2; remainder is seq only -> name ""."""
        zone = Zone("1.0.0.127.in-addr.arpa.", [])
        client = DictEtcdClient(
            {"/skydns/arpa/in-addr/127/0/0/1/2": {"host": "ptr.example.com", "ttl": 60}}
        )
        provider = EtcdProvider("etcd", client=client)
        provider.populate(zone, target=False, lenient=False)
        assert len(zone.records) == 1
        rec = list(zone.records)[0]
        assert rec.name == ""
        assert rec._type == "PTR"
        assert list(rec.values) == ["ptr.example.com."]

    def test_populate_ptr_subnet_zone_0_0_127(self) -> None:
        """Zone 0.0.127.in-addr.arpa., key arpa/in-addr/127/0/0/1; remainder "1" -> name "1"."""
        zone = Zone("0.0.127.in-addr.arpa.", [])
        client = DictEtcdClient(
            {"/skydns/arpa/in-addr/127/0/0/1": {"host": "host1.example.com", "ttl": 60}}
        )
        provider = EtcdProvider("etcd", client=client)
        provider.populate(zone, target=False, lenient=False)
        assert len(zone.records) == 1
        rec = list(zone.records)[0]
        assert rec.name == "1"
        assert rec._type == "PTR"
        assert list(rec.values) == ["host1.example.com."]

    def test_populate_ptr_slash16_two_records(self) -> None:
        """When 1.2.0.0/16 has two PTRs (1.2.3.4 and 1.2.3.5), both are populated."""
        zone = Zone("2.1.in-addr.arpa.", [])  # 1.2.0.0/16
        client = DictEtcdClient(
            {
                "/skydns/arpa/in-addr/1/2/3/4": {
                    "host": "host4.example.com",
                    "ttl": 60,
                },
                "/skydns/arpa/in-addr/1/2/3/5": {
                    "host": "host5.example.com",
                    "ttl": 60,
                },
            }
        )
        provider = EtcdProvider("etcd", client=client)
        provider.populate(zone, target=False, lenient=False)
        assert len(zone.records) == 2
        by_name = {rec.name: rec for rec in zone.records}
        assert "4.3" in by_name
        assert "5.3" in by_name
        assert by_name["4.3"]._type == "PTR"
        assert list(by_name["4.3"].values) == ["host4.example.com."]
        assert by_name["5.3"]._type == "PTR"
        assert list(by_name["5.3"].values) == ["host5.example.com."]
