"""Tests for RelayManager — single-relay connection with failover and backoff.

Covers: construction, connect/disconnect, failover round-robin,
        exponential backoff, failure/success tracking, state queries.
"""
import asyncio
import pytest
import pytest_asyncio

from satorilib.satori_nostr.relay import RelayManager, RelayError
from tests.mini_relay import MiniRelay


# =========================================================================
# Helpers
# =========================================================================

@pytest_asyncio.fixture
async def relay():
    """Single mini relay."""
    r = MiniRelay()
    await r.start()
    yield r
    await r.stop()


@pytest_asyncio.fixture
async def two_relays():
    """Two mini relays for failover tests."""
    r1 = MiniRelay()
    r2 = MiniRelay()
    await r1.start()
    await r2.start()
    yield r1, r2
    await r1.stop()
    await r2.stop()


# =========================================================================
# Construction
# =========================================================================

class TestConstruction:
    def test_create(self):
        m = RelayManager(relay_urls=["wss://r1.com"])
        assert m.relay_urls == ["wss://r1.com"]
        assert m.soft_timeout_ms == 8000
        assert m.max_backoff_s == 30.0

    def test_custom_params(self):
        m = RelayManager(
            relay_urls=["wss://r1.com"],
            soft_timeout_ms=5000,
            max_backoff_s=10.0,
        )
        assert m.soft_timeout_ms == 5000
        assert m.max_backoff_s == 10.0

    def test_empty_urls_raises(self):
        with pytest.raises(RelayError, match="at least one relay"):
            RelayManager(relay_urls=[])


# =========================================================================
# State before connect
# =========================================================================

class TestInitialState:
    def test_not_connected(self):
        m = RelayManager(relay_urls=["wss://r1.com"])
        assert m.is_connected() is False

    def test_no_active_relay(self):
        m = RelayManager(relay_urls=["wss://r1.com"])
        assert m.get_active_relay() is None


# =========================================================================
# Connect / Disconnect
# =========================================================================

class TestConnect:
    @pytest.mark.asyncio
    async def test_connect(self, relay):
        m = RelayManager(relay_urls=[relay.url])
        await m.connect()
        assert m.is_connected() is True
        assert m.get_active_relay() == relay.url
        await m.disconnect()

    @pytest.mark.asyncio
    async def test_disconnect(self, relay):
        m = RelayManager(relay_urls=[relay.url])
        await m.connect()
        await m.disconnect()
        assert m.is_connected() is False
        assert m.get_active_relay() is None

    @pytest.mark.asyncio
    async def test_disconnect_when_not_connected(self):
        m = RelayManager(relay_urls=["wss://r1.com"])
        await m.disconnect()  # should not raise

    @pytest.mark.asyncio
    async def test_connect_resets_backoff(self, relay):
        m = RelayManager(relay_urls=[relay.url])
        # Manually add failures
        m._record_failure(relay.url)
        m._record_failure(relay.url)
        assert m._failure_counts[relay.url] == 2
        # Connect resets
        await m.connect()
        assert m._failure_counts[relay.url] == 0
        await m.disconnect()

    @pytest.mark.asyncio
    async def test_connect_to_unreachable_still_marks_connected(self):
        """nostr-sdk connects async in background; connect() doesn't raise."""
        m = RelayManager(relay_urls=["wss://127.0.0.1:1"])
        await m.connect()
        # nostr-sdk considers this "connected" even if relay is unreachable
        assert m.is_connected() is True
        await m.disconnect()

    def test_record_failure_tracks_count(self):
        m = RelayManager(relay_urls=["wss://r1.com"])
        m._record_failure("wss://r1.com")
        assert m._failure_counts["wss://r1.com"] == 1
        m._record_failure("wss://r1.com")
        assert m._failure_counts["wss://r1.com"] == 2

    @pytest.mark.asyncio
    async def test_reconnect_after_disconnect(self, relay):
        m = RelayManager(relay_urls=[relay.url])
        await m.connect()
        await m.disconnect()
        await m.connect()
        assert m.is_connected() is True
        await m.disconnect()


# =========================================================================
# Failover
# =========================================================================

class TestFailover:
    @pytest.mark.asyncio
    async def test_failover_to_next(self, two_relays):
        r1, r2 = two_relays
        m = RelayManager(relay_urls=[r1.url, r2.url])
        await m.connect()
        assert m.get_active_relay() == r1.url
        await m.failover()
        assert m.get_active_relay() == r2.url
        await m.disconnect()

    @pytest.mark.asyncio
    async def test_failover_wraps_around(self, two_relays):
        r1, r2 = two_relays
        m = RelayManager(relay_urls=[r1.url, r2.url])
        await m.connect()
        await m.failover()  # -> r2
        await m.failover()  # -> r1 (wrap)
        assert m.get_active_relay() == r1.url
        await m.disconnect()

    @pytest.mark.asyncio
    async def test_failover_single_relay(self, relay):
        m = RelayManager(relay_urls=[relay.url])
        await m.connect()
        await m.failover()  # wraps to same relay
        assert m.get_active_relay() == relay.url
        await m.disconnect()


# =========================================================================
# Backoff
# =========================================================================

class TestBackoff:
    def test_no_failures(self):
        m = RelayManager(relay_urls=["wss://r1.com"])
        assert m._get_backoff_delay("wss://r1.com") == 1.0  # 2^0

    def test_exponential_progression(self):
        m = RelayManager(relay_urls=["wss://r1.com"], max_backoff_s=100.0)
        m._failure_counts["wss://r1.com"] = 0
        assert m._get_backoff_delay("wss://r1.com") == 1.0   # 2^0
        m._failure_counts["wss://r1.com"] = 1
        assert m._get_backoff_delay("wss://r1.com") == 2.0   # 2^1
        m._failure_counts["wss://r1.com"] = 2
        assert m._get_backoff_delay("wss://r1.com") == 4.0   # 2^2
        m._failure_counts["wss://r1.com"] = 3
        assert m._get_backoff_delay("wss://r1.com") == 8.0   # 2^3

    def test_capped_at_max(self):
        m = RelayManager(relay_urls=["wss://r1.com"], max_backoff_s=10.0)
        m._failure_counts["wss://r1.com"] = 20  # 2^20 >> 10
        assert m._get_backoff_delay("wss://r1.com") == 10.0

    def test_record_failure_increments(self):
        m = RelayManager(relay_urls=["wss://r1.com"])
        m._record_failure("wss://r1.com")
        assert m._failure_counts["wss://r1.com"] == 1
        m._record_failure("wss://r1.com")
        assert m._failure_counts["wss://r1.com"] == 2

    def test_record_success_resets(self):
        m = RelayManager(relay_urls=["wss://r1.com"])
        m._record_failure("wss://r1.com")
        m._record_failure("wss://r1.com")
        m._record_success("wss://r1.com")
        assert m._failure_counts["wss://r1.com"] == 0
