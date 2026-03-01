"""Integration tests with OctoDNS: plan → apply → populate round-trip."""

from __future__ import annotations

import pytest
from octodns.record import Record
from octodns.zone import Zone

from octodns_etcd.provider import EtcdProvider
from tests.conftest import DictEtcdClient


@pytest.fixture
def client() -> DictEtcdClient:
    return DictEtcdClient({})


@pytest.fixture
def provider(client: DictEtcdClient) -> EtcdProvider:
    return EtcdProvider("etcd", client=client)


@pytest.fixture
def zone() -> Zone:
    return Zone("example.com.", [])


def _desired(zone: Zone, *records: Record) -> Zone:
    """Return a desired zone with the given records added."""
    desired = Zone(zone.name, [])
    for rec in records:
        desired.add_record(rec)
    return desired


class TestRoundTrip:
    """Round-trip tests: plan → apply → populate."""

    def test_create_a_record(
        self, provider: EtcdProvider, client: DictEtcdClient, zone: Zone
    ) -> None:
        """Adding an A record writes to etcd and populate reads it back."""
        desired = _desired(
            zone,
            Record.new(zone, "www", {"type": "A", "ttl": 300, "values": ["1.2.3.4"]}),
        )

        plan = provider.plan(desired)
        assert plan is not None
        provider.apply(plan)

        result = Zone(zone.name, [])
        provider.populate(result)
        assert len(result.records) == 1
        rec = list(result.records)[0]
        assert rec.name == "www"
        assert rec._type == "A"
        assert list(rec.values) == ["1.2.3.4"]

    def test_create_multiple_a_values(
        self, provider: EtcdProvider, client: DictEtcdClient, zone: Zone
    ) -> None:
        """Multiple A values are written as multiple keys and merged on populate."""
        desired = _desired(
            zone,
            Record.new(
                zone, "www", {"type": "A", "ttl": 300, "values": ["1.2.3.4", "1.2.3.5"]}
            ),
        )

        plan = provider.plan(desired)
        assert plan is not None
        provider.apply(plan)

        result = Zone(zone.name, [])
        provider.populate(result)
        rec = list(result.records)[0]
        assert rec._type == "A"
        assert set(rec.values) == {"1.2.3.4", "1.2.3.5"}

    def test_create_txt_record(
        self, provider: EtcdProvider, client: DictEtcdClient, zone: Zone
    ) -> None:
        """TXT record round-trips."""
        desired = _desired(
            zone,
            Record.new(
                zone,
                "txt",
                {
                    "type": "TXT",
                    "ttl": 60,
                    "values": ["v=spf1 include:example.com ~all"],
                },
            ),
        )

        plan = provider.plan(desired)
        assert plan is not None
        provider.apply(plan)

        result = Zone(zone.name, [])
        provider.populate(result)
        rec = list(result.records)[0]
        assert rec._type == "TXT"
        assert list(rec.values) == ["v=spf1 include:example.com ~all"]

    def test_create_cname_record(
        self, provider: EtcdProvider, client: DictEtcdClient, zone: Zone
    ) -> None:
        """CNAME record round-trips."""
        desired = _desired(
            zone,
            Record.new(
                zone,
                "alias",
                {"type": "CNAME", "ttl": 300, "value": "target.example.com."},
            ),
        )

        plan = provider.plan(desired)
        assert plan is not None
        provider.apply(plan)

        result = Zone(zone.name, [])
        provider.populate(result)
        rec = list(result.records)[0]
        assert rec._type == "CNAME"
        assert rec.value == "target.example.com."

    def test_update_a_record(
        self, provider: EtcdProvider, client: DictEtcdClient, zone: Zone
    ) -> None:
        """Updating an A record removes old value; populate returns the new one."""
        # Create initial state
        initial = _desired(
            zone,
            Record.new(zone, "www", {"type": "A", "ttl": 300, "values": ["1.2.3.4"]}),
        )
        provider.apply(provider.plan(initial))

        # Change value
        updated = _desired(
            zone,
            Record.new(zone, "www", {"type": "A", "ttl": 300, "values": ["5.6.7.8"]}),
        )
        plan = provider.plan(updated)
        assert plan is not None
        provider.apply(plan)

        result = Zone(zone.name, [])
        provider.populate(result)
        rec = list(result.records)[0]
        assert list(rec.values) == ["5.6.7.8"]

    def test_delete_a_record(
        self, provider: EtcdProvider, client: DictEtcdClient, zone: Zone
    ) -> None:
        """Deleting an A record makes it no longer returned by populate."""
        initial = _desired(
            zone,
            Record.new(zone, "www", {"type": "A", "ttl": 300, "values": ["1.2.3.4"]}),
        )
        provider.apply(provider.plan(initial))

        plan = provider.plan(Zone(zone.name, []))
        assert plan is not None
        provider.apply(plan)

        result = Zone(zone.name, [])
        provider.populate(result)
        assert len(result.records) == 0

    def test_no_change_plan_is_none(
        self, provider: EtcdProvider, client: DictEtcdClient, zone: Zone
    ) -> None:
        """Planning the same content twice yields None the second time."""
        desired = _desired(
            zone,
            Record.new(zone, "www", {"type": "A", "ttl": 300, "values": ["1.2.3.4"]}),
        )
        provider.apply(provider.plan(desired))

        plan = provider.plan(desired)
        assert plan is None

    def test_create_multiple_record_types(
        self, provider: EtcdProvider, client: DictEtcdClient, zone: Zone
    ) -> None:
        """Apply A and TXT together; populate returns both."""
        desired = _desired(
            zone,
            Record.new(zone, "www", {"type": "A", "ttl": 300, "values": ["1.2.3.4"]}),
            Record.new(zone, "info", {"type": "TXT", "ttl": 60, "values": ["hello"]}),
        )

        plan = provider.plan(desired)
        assert plan is not None
        provider.apply(plan)

        result = Zone(zone.name, [])
        provider.populate(result)
        assert len(result.records) == 2
        by_name = {rec.name: rec for rec in result.records}
        assert by_name["www"]._type == "A"
        assert by_name["info"]._type == "TXT"
