"""Tests for satori_nostr models.

Covers: DatastreamMetadata, DatastreamObservation, SubscriptionAnnouncement,
        PaymentNotification, InboundObservation, InboundPayment,
        SatoriNostrConfig, constants, compute_stream_topic_tag
"""
import time
import json
import pytest

from satorilib.satori_nostr.models import (
    DatastreamMetadata,
    DatastreamObservation,
    SubscriptionAnnouncement,
    PaymentNotification,
    InboundObservation,
    InboundPayment,
    SatoriNostrConfig,
    KIND_DATASTREAM_ANNOUNCE,
    KIND_DATASTREAM_DATA,
    KIND_SUBSCRIPTION_ANNOUNCE,
    KIND_PAYMENT,
    CADENCE_REALTIME,
    CADENCE_MINUTE,
    CADENCE_5MIN,
    CADENCE_HOURLY,
    CADENCE_DAILY,
    CADENCE_WEEKLY,
    CADENCE_IRREGULAR,
    compute_stream_topic_tag,
)


# =========================================================================
# Constants
# =========================================================================

class TestConstants:
    def test_kind_values(self):
        assert KIND_DATASTREAM_ANNOUNCE == 30100
        assert KIND_DATASTREAM_DATA == 30101
        assert KIND_SUBSCRIPTION_ANNOUNCE == 30102
        assert KIND_PAYMENT == 30103

    def test_cadence_values(self):
        assert CADENCE_REALTIME == 1
        assert CADENCE_MINUTE == 60
        assert CADENCE_5MIN == 300
        assert CADENCE_HOURLY == 3600
        assert CADENCE_DAILY == 86400
        assert CADENCE_WEEKLY == 604800
        assert CADENCE_IRREGULAR is None

    def test_compute_stream_topic_tag(self):
        assert compute_stream_topic_tag("btc-price") == "satori:stream:btc-price"
        assert compute_stream_topic_tag("weather-nyc") == "satori:stream:weather-nyc"
        assert compute_stream_topic_tag("") == "satori:stream:"


# =========================================================================
# DatastreamMetadata
# =========================================================================

def make_metadata(**overrides):
    """Helper to create a DatastreamMetadata with sensible defaults."""
    defaults = dict(
        stream_name="btc-price",
        nostr_pubkey="aabbcc" * 10 + "aabb",
        name="Bitcoin Price",
        description="BTC/USD price feed",
        encrypted=False,
        price_per_obs=0,
        created_at=1700000000,
        cadence_seconds=3600,
        tags=["bitcoin", "price"],
        metadata=None,
    )
    defaults.update(overrides)
    return DatastreamMetadata(**defaults)


class TestDatastreamMetadata:
    def test_create(self):
        m = make_metadata()
        assert m.stream_name == "btc-price"
        assert m.encrypted is False
        assert m.price_per_obs == 0
        assert m.cadence_seconds == 3600

    def test_to_dict(self):
        m = make_metadata()
        d = m.to_dict()
        assert d["stream_name"] == "btc-price"
        assert d["encrypted"] is False
        assert d["tags"] == ["bitcoin", "price"]
        assert d["metadata"] is None

    def test_from_dict(self):
        m = make_metadata()
        d = m.to_dict()
        m2 = DatastreamMetadata.from_dict(d)
        assert m2.stream_name == m.stream_name
        assert m2.price_per_obs == m.price_per_obs
        assert m2.tags == m.tags

    def test_to_json_roundtrip(self):
        m = make_metadata(metadata={"source": "coinbase"})
        j = m.to_json()
        m2 = DatastreamMetadata.from_json(j)
        assert m2.stream_name == m.stream_name
        assert m2.metadata == {"source": "coinbase"}

    def test_to_json_is_valid_json(self):
        m = make_metadata()
        parsed = json.loads(m.to_json())
        assert parsed["stream_name"] == "btc-price"

    def test_uuid_deterministic(self):
        m = make_metadata()
        u1 = m.uuid
        u2 = m.uuid
        assert u1 == u2

    def test_uuid_different_streams(self):
        m1 = make_metadata(stream_name="btc-price")
        m2 = make_metadata(stream_name="eth-price")
        assert m1.uuid != m2.uuid

    def test_uuid_different_pubkeys(self):
        m1 = make_metadata(nostr_pubkey="aa" * 32)
        m2 = make_metadata(nostr_pubkey="bb" * 32)
        assert m1.uuid != m2.uuid

    def test_uuid_format(self):
        m = make_metadata()
        # UUID v5 format: 8-4-4-4-12 hex chars
        parts = m.uuid.split("-")
        assert len(parts) == 5
        assert [len(p) for p in parts] == [8, 4, 4, 4, 12]

    def test_is_likely_active_within_cadence(self):
        m = make_metadata(cadence_seconds=3600)
        # Last observation 30 minutes ago
        last_obs = int(time.time()) - 1800
        assert m.is_likely_active(last_obs) is True

    def test_is_likely_active_stale(self):
        m = make_metadata(cadence_seconds=3600)
        # Last observation 3 hours ago (> 2x cadence)
        last_obs = int(time.time()) - 10800
        assert m.is_likely_active(last_obs) is False

    def test_is_likely_active_irregular(self):
        m = make_metadata(cadence_seconds=None)
        # Last observation 12 hours ago (< 24h default)
        last_obs = int(time.time()) - 43200
        assert m.is_likely_active(last_obs) is True

    def test_is_likely_active_irregular_stale(self):
        m = make_metadata(cadence_seconds=None)
        # Last observation 25 hours ago (> 24h)
        last_obs = int(time.time()) - 90000
        assert m.is_likely_active(last_obs) is False

    def test_is_likely_active_custom_multiplier(self):
        m = make_metadata(cadence_seconds=3600)
        # Last observation 2.5 hours ago
        last_obs = int(time.time()) - 9000
        # With default 2.0 multiplier: stale (9000 > 7200)
        assert m.is_likely_active(last_obs, max_staleness_multiplier=2.0) is False
        # With 3.0 multiplier: active (9000 < 10800)
        assert m.is_likely_active(last_obs, max_staleness_multiplier=3.0) is True

    def test_paid_stream(self):
        m = make_metadata(encrypted=True, price_per_obs=100)
        assert m.encrypted is True
        assert m.price_per_obs == 100

    def test_free_encrypted_stream(self):
        """Free stream can still be encrypted."""
        m = make_metadata(encrypted=True, price_per_obs=0)
        assert m.encrypted is True
        assert m.price_per_obs == 0


# =========================================================================
# DatastreamObservation
# =========================================================================

def make_observation(**overrides):
    defaults = dict(
        stream_name="btc-price",
        timestamp=1700000000,
        value={"price": 45000.0, "volume": 1234},
        seq_num=1,
    )
    defaults.update(overrides)
    return DatastreamObservation(**defaults)


class TestDatastreamObservation:
    def test_create(self):
        o = make_observation()
        assert o.stream_name == "btc-price"
        assert o.seq_num == 1
        assert o.value["price"] == 45000.0

    def test_to_dict(self):
        o = make_observation()
        d = o.to_dict()
        assert d["stream_name"] == "btc-price"
        assert d["seq_num"] == 1

    def test_from_dict(self):
        o = make_observation()
        o2 = DatastreamObservation.from_dict(o.to_dict())
        assert o2.stream_name == o.stream_name
        assert o2.value == o.value

    def test_json_roundtrip(self):
        o = make_observation(value="simple string value")
        j = o.to_json()
        o2 = DatastreamObservation.from_json(j)
        assert o2.value == "simple string value"
        assert o2.seq_num == o.seq_num

    def test_various_value_types(self):
        """Value can be dict, string, number, list."""
        for val in [42, 3.14, "hello", [1, 2, 3], {"nested": {"deep": True}}]:
            o = make_observation(value=val)
            o2 = DatastreamObservation.from_json(o.to_json())
            assert o2.value == val


# =========================================================================
# SubscriptionAnnouncement
# =========================================================================

def make_subscription(**overrides):
    defaults = dict(
        subscriber_pubkey="sub" + "aa" * 30 + "bb",
        stream_name="btc-price",
        nostr_pubkey="pub" + "cc" * 30 + "dd",
        timestamp=1700000000,
        payment_channel=None,
    )
    defaults.update(overrides)
    return SubscriptionAnnouncement(**defaults)


class TestSubscriptionAnnouncement:
    def test_create(self):
        s = make_subscription()
        assert s.stream_name == "btc-price"
        assert s.payment_channel is None

    def test_json_roundtrip(self):
        s = make_subscription(payment_channel="lnbc1...")
        s2 = SubscriptionAnnouncement.from_json(s.to_json())
        assert s2.subscriber_pubkey == s.subscriber_pubkey
        assert s2.payment_channel == "lnbc1..."

    def test_to_dict(self):
        s = make_subscription()
        d = s.to_dict()
        assert "subscriber_pubkey" in d
        assert "nostr_pubkey" in d


# =========================================================================
# PaymentNotification
# =========================================================================

def make_payment(**overrides):
    defaults = dict(
        from_pubkey="from" + "aa" * 30,
        to_pubkey="to" + "bb" * 31,
        stream_name="btc-price",
        seq_num=42,
        amount_sats=100,
        timestamp=1700000000,
        tx_id=None,
    )
    defaults.update(overrides)
    return PaymentNotification(**defaults)


class TestPaymentNotification:
    def test_create(self):
        p = make_payment()
        assert p.amount_sats == 100
        assert p.seq_num == 42
        assert p.tx_id is None

    def test_json_roundtrip(self):
        p = make_payment(tx_id="lntx_abc123")
        p2 = PaymentNotification.from_json(p.to_json())
        assert p2.amount_sats == p.amount_sats
        assert p2.tx_id == "lntx_abc123"

    def test_to_dict(self):
        p = make_payment()
        d = p.to_dict()
        assert d["amount_sats"] == 100
        assert d["tx_id"] is None


# =========================================================================
# InboundObservation / InboundPayment
# =========================================================================

class TestInboundTypes:
    def test_inbound_observation(self):
        obs = make_observation()
        inbound = InboundObservation(
            stream_name="btc-price",
            nostr_pubkey="aa" * 32,
            observation=obs,
            event_id="event123",
        )
        assert inbound.stream_name == "btc-price"
        assert inbound.observation.value == obs.value
        assert inbound.event_id == "event123"
        assert inbound.raw_event is None

    def test_inbound_observation_with_raw(self):
        obs = make_observation()
        raw = {"id": "abc", "kind": 30101}
        inbound = InboundObservation(
            stream_name="btc-price",
            nostr_pubkey="aa" * 32,
            observation=obs,
            event_id="event123",
            raw_event=raw,
        )
        assert inbound.raw_event == raw

    def test_inbound_payment(self):
        pay = make_payment()
        inbound = InboundPayment(
            payment=pay,
            event_id="event456",
        )
        assert inbound.payment.amount_sats == 100
        assert inbound.event_id == "event456"
        assert inbound.raw_event is None


# =========================================================================
# SatoriNostrConfig
# =========================================================================

class TestSatoriNostrConfig:
    def test_create_with_hex_key(self):
        c = SatoriNostrConfig(
            keys="aa" * 32,
            relay_urls=["wss://relay.example.com"],
        )
        assert c.keys == "aa" * 32
        assert c.relay_urls == ["wss://relay.example.com"]

    def test_defaults(self):
        c = SatoriNostrConfig(keys="aa" * 32, relay_urls=["wss://r.com"])
        assert c.active_relay_timeout_ms == 8000
        assert c.dedupe_db_path is None

    def test_multiple_relays(self):
        c = SatoriNostrConfig(
            keys="aa" * 32,
            relay_urls=["wss://r1.com", "wss://r2.com", "wss://r3.com"],
        )
        assert len(c.relay_urls) == 3
