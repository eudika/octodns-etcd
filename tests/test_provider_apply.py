"""Tests for EtcdProvider._delete_existing_records, _create_new_records, _apply."""

from __future__ import annotations

import pytest
from octodns.provider.plan import Plan
from octodns.record import Create, Delete, Record, Update
from octodns.zone import Zone

from octodns_etcd.provider import EtcdProvider
from tests.conftest import DictEtcdClient


@pytest.fixture
def zone() -> Zone:
    return Zone("example.com.", [])


@pytest.fixture
def client() -> DictEtcdClient:
    return DictEtcdClient({})


@pytest.fixture
def provider(client: DictEtcdClient) -> EtcdProvider:
    return EtcdProvider("etcd", client=client)


class TestDeleteExistingRecords:
    """Tests for _delete_existing_records."""

    def test_deletes_single_key(
        self, provider: EtcdProvider, client: DictEtcdClient, zone: Zone
    ) -> None:
        """Delete the single key in etcd."""
        client.data["/skydns/com/example/app"] = {"host": "1.2.3.4"}
        record = Record.new(
            zone, "app", {"type": "A", "ttl": 300, "values": ["1.2.3.4"]}
        )

        count = provider._delete_existing_records(record)

        assert count == 1
        assert client.data == {}

    def test_deletes_multiple_keys_under_same_prefix(
        self, provider: EtcdProvider, client: DictEtcdClient, zone: Zone
    ) -> None:
        """Delete all keys under the same prefix."""
        client.data["/skydns/com/example/app/0"] = {"host": "1.2.3.4"}
        client.data["/skydns/com/example/app/1"] = {"host": "1.2.3.5"}
        record = Record.new(
            zone, "app", {"type": "A", "ttl": 300, "values": ["1.2.3.4", "1.2.3.5"]}
        )

        count = provider._delete_existing_records(record)

        assert count == 2
        assert client.data == {}

    def test_returns_zero_when_no_keys(
        self, provider: EtcdProvider, client: DictEtcdClient, zone: Zone
    ) -> None:
        """Return 0 when no keys match."""
        record = Record.new(
            zone, "app", {"type": "A", "ttl": 300, "values": ["1.2.3.4"]}
        )

        count = provider._delete_existing_records(record)

        assert count == 0
        assert client.data == {}


class TestCreateNewRecords:
    """Tests for _create_new_records."""

    def test_creates_single_a_record(
        self, provider: EtcdProvider, client: DictEtcdClient, zone: Zone
    ) -> None:
        """Put single A record to etcd; no seq suffix when only one record."""
        record = Record.new(
            zone, "app", {"type": "A", "ttl": 300, "values": ["1.2.3.4"]}
        )

        count = provider._create_new_records(record)

        assert count == 1
        assert client.data == {
            "/skydns/com/example/app": {"host": "1.2.3.4", "ttl": 300}
        }

    def test_creates_multiple_a_values_as_separate_keys(
        self, provider: EtcdProvider, client: DictEtcdClient, zone: Zone
    ) -> None:
        """Put each A value to a separate key."""
        record = Record.new(
            zone, "app", {"type": "A", "ttl": 300, "values": ["1.2.3.4", "1.2.3.5"]}
        )

        count = provider._create_new_records(record)

        assert count == 2
        assert client.data == {
            "/skydns/com/example/app/0": {"host": "1.2.3.4", "ttl": 300},
            "/skydns/com/example/app/1": {"host": "1.2.3.5", "ttl": 300},
        }

    def test_creates_txt_record(
        self, provider: EtcdProvider, client: DictEtcdClient, zone: Zone
    ) -> None:
        """Put TXT record to etcd."""
        record = Record.new(
            zone, "app", {"type": "TXT", "ttl": 300, "values": ["hello"]}
        )

        count = provider._create_new_records(record)

        assert count == 1
        assert client.data == {"/skydns/com/example/app": {"text": "hello", "ttl": 300}}

    def test_creates_ptr_record_in_addr_arpa(
        self, provider: EtcdProvider, client: DictEtcdClient
    ) -> None:
        """Put PTR record (IPv4 in-addr.arpa) to etcd."""
        zone = Zone("1.0.0.127.in-addr.arpa.", [])
        record = Record.new(
            zone, "", {"type": "PTR", "ttl": 60, "values": ["ptr.example.com."]}
        )

        count = provider._create_new_records(record)

        assert count == 1
        assert client.data == {
            "/skydns/arpa/in-addr/127/0/0/1": {"host": "ptr.example.com.", "ttl": 60},
        }

    def test_creates_ptr_record_ip6_arpa(
        self, provider: EtcdProvider, client: DictEtcdClient
    ) -> None:
        """Put PTR record (IPv6 ip6.arpa, ::1 = 32 nibbles) to etcd."""
        # ::1 reverse = 1 + 31 zeros (32 nibbles). "0."*31 gives 31 zeros.
        zone = Zone("1." + "0." * 31 + "ip6.arpa.", [])
        record = Record.new(
            zone, "", {"type": "PTR", "ttl": 60, "values": ["ptr.example.com."]}
        )

        provider._create_new_records(record)

        # _name_to_key: FQDN split by ".", reversed → arpa/ip6/0/.../0/1 (31 zeros + 1). No seq for single record.
        base = "/skydns/arpa/ip6/" + "0/" * 31 + "1"
        assert client.data == {base: {"host": "ptr.example.com.", "ttl": 60}}

    def test_unsupported_type_returns_zero(
        self, provider: EtcdProvider, client: DictEtcdClient, zone: Zone
    ) -> None:
        """Return 0 and do not put when type not in SUPPORTS."""
        record = Record.new(
            zone, "app", {"type": "A", "ttl": 300, "values": ["1.2.3.4"]}
        )
        provider.SUPPORTS = set()

        count = provider._create_new_records(record)

        assert count == 0
        assert client.data == {}

    def test_apply_create_two_values_when_only_slash_one_exists_appends_after(
        self, provider: EtcdProvider, client: DictEtcdClient, zone: Zone
    ) -> None:
        """When only ../app/1 exists, adding 2 records appends at /2 and /3."""
        client.data["/skydns/com/example/app/1"] = {
            "host": "192.168.1.1",
            "ttl": 300,
        }
        record = Record.new(
            zone,
            "app",
            {"type": "A", "ttl": 300, "values": ["10.0.0.1", "10.0.0.2"]},
        )

        count = provider._create_new_records(record)

        assert count == 2
        assert client.data == {
            "/skydns/com/example/app/1": {"host": "192.168.1.1", "ttl": 300},
            "/skydns/com/example/app/2": {"host": "10.0.0.1", "ttl": 300},
            "/skydns/com/example/app/3": {"host": "10.0.0.2", "ttl": 300},
        }


class TestApply:
    """Tests for _apply."""

    def test_apply_create_adds_records(
        self, provider: EtcdProvider, client: DictEtcdClient, zone: Zone
    ) -> None:
        """Create-only plan: records are put to etcd."""
        record = Record.new(
            zone, "app", {"type": "A", "ttl": 300, "values": ["1.2.3.4"]}
        )

        count = provider._apply(Plan(None, None, [Create(record)], True))

        assert count == 1
        assert client.data == {
            "/skydns/com/example/app": {"host": "1.2.3.4", "ttl": 300}
        }

    def test_apply_create_same_name_a_and_aaaa_both_stored(
        self, provider: EtcdProvider, client: DictEtcdClient, zone: Zone
    ) -> None:
        """Creating A and AAAA for same name stores both in etcd (aggregated by name)."""
        plan = Plan(
            None,
            None,
            [
                Create(
                    Record.new(
                        zone, "app", {"type": "A", "ttl": 300, "values": ["1.2.3.4"]}
                    )
                ),
                Create(
                    Record.new(
                        zone,
                        "app",
                        {"type": "AAAA", "ttl": 300, "values": ["2001:db8::1"]},
                    )
                ),
            ],
            True,
        )

        count = provider._apply(plan)

        assert count == 2
        assert client.data == {
            "/skydns/com/example/app/0": {"host": "1.2.3.4", "ttl": 300},
            "/skydns/com/example/app/1": {"host": "2001:db8::1", "ttl": 300},
        }

    def test_apply_create_after_existing_bare_key_appends_seq(
        self, provider: EtcdProvider, client: DictEtcdClient, zone: Zone
    ) -> None:
        """When ../app (no suffix) exists, migrate to ../app/0 and append new at ../app/1."""
        client.data["/skydns/com/example/app"] = {"host": "1.2.3.4"}
        record = Record.new(
            zone, "app", {"type": "AAAA", "ttl": 300, "values": ["2001:db8::1"]}
        )

        count = provider._create_new_records(record)

        assert count == 1
        assert client.data == {
            "/skydns/com/example/app/0": {"host": "1.2.3.4"},
            "/skydns/com/example/app/1": {"host": "2001:db8::1", "ttl": 300},
        }

    def test_apply_create_after_existing_seq_key_appends_next_seq(
        self, provider: EtcdProvider, client: DictEtcdClient, zone: Zone
    ) -> None:
        """When ../app/0 exists, new record is stored at ../app/1."""
        client.data["/skydns/com/example/app/0"] = {"host": "1.2.3.4", "ttl": 300}
        record = Record.new(
            zone, "app", {"type": "AAAA", "ttl": 300, "values": ["2001:db8::1"]}
        )

        count = provider._create_new_records(record)

        assert count == 1
        assert client.data == {
            "/skydns/com/example/app/0": {"host": "1.2.3.4", "ttl": 300},
            "/skydns/com/example/app/1": {"host": "2001:db8::1", "ttl": 300},
        }

    def test_apply_delete_removes_records(
        self, provider: EtcdProvider, client: DictEtcdClient, zone: Zone
    ) -> None:
        """Delete-only plan: records are removed from etcd."""
        client.data["/skydns/com/example/app"] = {"host": "1.2.3.4"}
        record = Record.new(
            zone, "app", {"type": "A", "ttl": 300, "values": ["1.2.3.4"]}
        )

        count = provider._apply(Plan(None, None, [Delete(record)], True))

        assert count == 1
        assert client.data == {}

    def test_apply_update_deletes_then_creates(
        self, provider: EtcdProvider, client: DictEtcdClient, zone: Zone
    ) -> None:
        """Update: delete existing records then create new ones."""
        client.data["/skydns/com/example/app/0"] = {"host": "1.2.3.4"}
        existing = Record.new(
            zone, "app", {"type": "A", "ttl": 300, "values": ["1.2.3.4"]}
        )
        new = Record.new(zone, "app", {"type": "A", "ttl": 300, "values": ["5.6.7.8"]})

        count = provider._apply(Plan(None, None, [Update(existing, new)], True))

        assert count == 2
        assert client.data == {
            "/skydns/com/example/app": {"host": "5.6.7.8", "ttl": 300}
        }
