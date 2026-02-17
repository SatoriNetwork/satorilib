"""Tests for StreamHealthMonitor integration.

Covers: health calculation (ACTIVE/STALE/DEAD/UNKNOWN), stream add/remove,
        health change callbacks, statistics, lifecycle.
"""
import asyncio
import time
import pytest
import pytest_asyncio

from nostr_sdk import Keys

from satorilib.satori_nostr import (
    SatoriNostr,
    SatoriNostrConfig,
    DatastreamMetadata,
    DatastreamObservation,
)
from satorilib.satori_nostr.integrations.stream_monitor import (
    StreamHealthMonitor,
    StreamHealth,
    StreamStatus,
)
from tests.mini_relay import MiniRelay


# =========================================================================
# Helpers
# =========================================================================

def make_metadata(stream_name="test-stream", cadence=3600, **overrides):
    defaults = dict(
        stream_name=stream_name,
        nostr_pubkey="aa" * 32,
        name="Test",
        description="Test stream",
        encrypted=False,
        price_per_obs=0,
        created_at=int(time.time()),
        cadence_seconds=cadence,
        tags=["test"],
        metadata=None,
    )
    defaults.update(overrides)
    return DatastreamMetadata(**defaults)


@pytest_asyncio.fixture
async def relay_and_client():
    """Provide a running mini relay + SatoriNostr client."""
    relay = MiniRelay()
    await relay.start()

    keys = Keys.generate()
    config = SatoriNostrConfig(
        keys=keys.secret_key().to_hex(),
        relay_urls=[relay.url],
    )
    client = SatoriNostr(config)
    await client.start()

    yield relay, client, keys

    await client.stop()
    await relay.stop()


# =========================================================================
# StreamHealth enum
# =========================================================================

class TestStreamHealthEnum:
    def test_values(self):
        assert StreamHealth.ACTIVE.value == "active"
        assert StreamHealth.STALE.value == "stale"
        assert StreamHealth.DEAD.value == "dead"
        assert StreamHealth.UNKNOWN.value == "unknown"


# =========================================================================
# StreamStatus dataclass
# =========================================================================

class TestStreamStatus:
    def test_defaults(self):
        m = make_metadata()
        s = StreamStatus(
            stream_name="test",
            nostr_pubkey="aa" * 32,
            metadata=m,
        )
        assert s.health == StreamHealth.UNKNOWN
        assert s.last_observation_time is None
        assert s.consecutive_stale_checks == 0


# =========================================================================
# Health calculation (pure logic, no relay)
# =========================================================================

class TestHealthCalculation:
    def _calc(self, cadence, age, irregular=False):
        """Helper: calculate health for given cadence and observation age."""
        m = make_metadata(cadence=None if irregular else cadence)
        status = StreamStatus(
            stream_name="test",
            nostr_pubkey="aa" * 32,
            metadata=m,
        )
        # Create monitor with a dummy client (won't be used for pure calc)
        monitor = StreamHealthMonitor.__new__(StreamHealthMonitor)
        last_time = int(time.time()) - age if age is not None else None
        return monitor._calculate_health(status, last_time)

    def test_active_within_cadence(self):
        # 30 min old, 1 hour cadence → active (< 2x)
        assert self._calc(3600, 1800) == StreamHealth.ACTIVE

    def test_active_at_boundary(self):
        # Just under 2x cadence
        assert self._calc(3600, 7199) == StreamHealth.ACTIVE

    def test_stale_beyond_2x(self):
        # 3 hours old, 1 hour cadence → stale (3x)
        assert self._calc(3600, 10800) == StreamHealth.STALE

    def test_stale_at_boundary(self):
        # Just under 5x cadence
        assert self._calc(3600, 17999) == StreamHealth.STALE

    def test_dead_beyond_5x(self):
        # 6 hours old, 1 hour cadence → dead (6x)
        assert self._calc(3600, 21600) == StreamHealth.DEAD

    def test_unknown_no_observation(self):
        assert self._calc(3600, None) == StreamHealth.UNKNOWN

    def test_irregular_active(self):
        # 12 hours old, no cadence → active (< 24h)
        assert self._calc(None, 43200, irregular=True) == StreamHealth.ACTIVE

    def test_irregular_stale(self):
        # 2 days old, no cadence → stale (< 5 days)
        assert self._calc(None, 172800, irregular=True) == StreamHealth.STALE

    def test_irregular_dead(self):
        # 6 days old, no cadence → dead (> 5 days)
        assert self._calc(None, 518400, irregular=True) == StreamHealth.DEAD

    def test_fast_cadence(self):
        # 5 min cadence, 2 min old → active
        assert self._calc(300, 120) == StreamHealth.ACTIVE
        # 5 min cadence, 20 min old → stale
        assert self._calc(300, 1200) == StreamHealth.STALE
        # 5 min cadence, 30 min old → dead
        assert self._calc(300, 1800) == StreamHealth.DEAD


# =========================================================================
# Monitor lifecycle (needs running client)
# =========================================================================

class TestMonitorLifecycle:
    @pytest.mark.asyncio
    async def test_start_stop(self, relay_and_client):
        _, client, _ = relay_and_client
        monitor = StreamHealthMonitor(client=client)
        await monitor.start()
        assert monitor._running is True
        await monitor.stop()
        assert monitor._running is False

    @pytest.mark.asyncio
    async def test_double_start_raises(self, relay_and_client):
        _, client, _ = relay_and_client
        monitor = StreamHealthMonitor(client=client)
        await monitor.start()
        try:
            with pytest.raises(RuntimeError):
                await monitor.start()
        finally:
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_start_requires_running_client(self):
        keys = Keys.generate()
        config = SatoriNostrConfig(
            keys=keys.secret_key().to_hex(),
            relay_urls=["ws://127.0.0.1:9999"],
        )
        client = SatoriNostr(config)
        # Client not started
        monitor = StreamHealthMonitor(client=client)
        with pytest.raises(RuntimeError, match="Client must be running"):
            await monitor.start()

    @pytest.mark.asyncio
    async def test_stop_when_not_running(self, relay_and_client):
        _, client, _ = relay_and_client
        monitor = StreamHealthMonitor(client=client)
        await monitor.stop()  # should not raise


# =========================================================================
# Stream management
# =========================================================================

class TestStreamManagement:
    @pytest.mark.asyncio
    async def test_add_stream(self, relay_and_client):
        _, client, _ = relay_and_client
        monitor = StreamHealthMonitor(client=client)
        await monitor.start()
        try:
            m = make_metadata(stream_name="btc-price")
            await monitor.add_stream(m)
            status = monitor.get_stream_status("btc-price")
            assert status is not None
            assert status.stream_name == "btc-price"
        finally:
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_add_stream_initial_check(self, relay_and_client):
        """Adding a stream triggers an initial health check."""
        _, client, _ = relay_and_client
        monitor = StreamHealthMonitor(client=client)
        await monitor.start()
        try:
            m = make_metadata()
            await monitor.add_stream(m)
            status = monitor.get_stream_status("test-stream")
            # Should have done initial check (last_check_time > 0)
            assert status.last_check_time > 0
        finally:
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_add_duplicate_noop(self, relay_and_client):
        _, client, _ = relay_and_client
        monitor = StreamHealthMonitor(client=client)
        await monitor.start()
        try:
            m = make_metadata()
            await monitor.add_stream(m)
            await monitor.add_stream(m)  # duplicate
            assert len(monitor.get_all_streams()) == 1
        finally:
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_remove_stream(self, relay_and_client):
        _, client, _ = relay_and_client
        monitor = StreamHealthMonitor(client=client)
        await monitor.start()
        try:
            m = make_metadata()
            await monitor.add_stream(m)
            monitor.remove_stream("test-stream")
            assert monitor.get_stream_status("test-stream") is None
        finally:
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_remove_nonexistent_noop(self, relay_and_client):
        _, client, _ = relay_and_client
        monitor = StreamHealthMonitor(client=client)
        await monitor.start()
        try:
            monitor.remove_stream("nope")  # should not raise
        finally:
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_get_all_streams(self, relay_and_client):
        _, client, _ = relay_and_client
        monitor = StreamHealthMonitor(client=client)
        await monitor.start()
        try:
            await monitor.add_stream(make_metadata(stream_name="s1"))
            await monitor.add_stream(make_metadata(stream_name="s2"))
            all_streams = monitor.get_all_streams()
            names = [s.stream_name for s in all_streams]
            assert "s1" in names
            assert "s2" in names
        finally:
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_get_streams_by_health(self, relay_and_client):
        _, client, _ = relay_and_client
        monitor = StreamHealthMonitor(client=client)
        await monitor.start()
        try:
            await monitor.add_stream(make_metadata(stream_name="s1"))
            # No observations published → UNKNOWN
            unknown = monitor.get_streams_by_health(StreamHealth.UNKNOWN)
            assert len(unknown) == 1
            assert unknown[0].stream_name == "s1"
        finally:
            await monitor.stop()


# =========================================================================
# Health check with real relay data
# =========================================================================

class TestHealthCheckWithRelay:
    @pytest.mark.asyncio
    async def test_stream_with_recent_observation_is_active(self, relay_and_client):
        """Stream with a recent observation should be ACTIVE."""
        relay, client, keys = relay_and_client

        # Publish an observation
        m = make_metadata(
            stream_name="active-stream",
            nostr_pubkey=keys.public_key().to_hex(),
        )
        obs = DatastreamObservation(
            stream_name="active-stream",
            timestamp=int(time.time()),
            value={"price": 100},
            seq_num=1,
        )
        await client.announce_datastream(m)
        await client.publish_observation(obs, m)
        await asyncio.sleep(0.3)

        monitor = StreamHealthMonitor(client=client)
        await monitor.start()
        try:
            await monitor.add_stream(m)
            status = monitor.get_stream_status("active-stream")
            assert status.health == StreamHealth.ACTIVE
        finally:
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_stream_without_observations_is_unknown(self, relay_and_client):
        _, client, _ = relay_and_client
        monitor = StreamHealthMonitor(client=client)
        await monitor.start()
        try:
            m = make_metadata(stream_name="empty-stream")
            await monitor.add_stream(m)
            status = monitor.get_stream_status("empty-stream")
            assert status.health == StreamHealth.UNKNOWN
        finally:
            await monitor.stop()


# =========================================================================
# Callbacks
# =========================================================================

class TestCallbacks:
    @pytest.mark.asyncio
    async def test_stale_callback(self, relay_and_client):
        _, client, _ = relay_and_client
        stale_streams = []

        async def on_stale(name):
            stale_streams.append(name)

        monitor = StreamHealthMonitor(
            client=client,
            on_stream_stale=on_stale,
        )
        await monitor.start()
        try:
            m = make_metadata(stream_name="cb-test")
            await monitor.add_stream(m)
            status = monitor.get_stream_status("cb-test")

            # Manually force a health transition: ACTIVE → STALE
            status.health = StreamHealth.ACTIVE
            await monitor._handle_health_change(
                "cb-test", StreamHealth.ACTIVE, StreamHealth.STALE)
            assert "cb-test" in stale_streams
        finally:
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_active_callback_on_revival(self, relay_and_client):
        _, client, _ = relay_and_client
        active_streams = []

        async def on_active(name):
            active_streams.append(name)

        monitor = StreamHealthMonitor(
            client=client,
            on_stream_active=on_active,
        )
        await monitor.start()
        try:
            m = make_metadata(stream_name="revival-test")
            await monitor.add_stream(m)

            # Simulate STALE → ACTIVE
            await monitor._handle_health_change(
                "revival-test", StreamHealth.STALE, StreamHealth.ACTIVE)
            assert "revival-test" in active_streams
        finally:
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_dead_callback(self, relay_and_client):
        _, client, _ = relay_and_client
        dead_streams = []

        async def on_dead(name):
            dead_streams.append(name)

        monitor = StreamHealthMonitor(
            client=client,
            on_stream_dead=on_dead,
        )
        await monitor.start()
        try:
            m = make_metadata(stream_name="dead-test")
            await monitor.add_stream(m)

            await monitor._handle_health_change(
                "dead-test", StreamHealth.STALE, StreamHealth.DEAD)
            assert "dead-test" in dead_streams
        finally:
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_no_callback_same_state(self, relay_and_client):
        """No callback fired when health doesn't change."""
        _, client, _ = relay_and_client
        calls = []

        async def on_stale(name):
            calls.append(name)

        monitor = StreamHealthMonitor(
            client=client,
            on_stream_stale=on_stale,
        )
        await monitor.start()
        try:
            # STALE → STALE: should not fire
            await monitor._handle_health_change(
                "test", StreamHealth.STALE, StreamHealth.STALE)
            assert len(calls) == 0
        finally:
            await monitor.stop()


# =========================================================================
# Statistics
# =========================================================================

class TestStatistics:
    @pytest.mark.asyncio
    async def test_stats(self, relay_and_client):
        _, client, _ = relay_and_client
        monitor = StreamHealthMonitor(client=client)
        await monitor.start()
        try:
            await monitor.add_stream(make_metadata(stream_name="s1"))
            stats = monitor.get_statistics()
            assert stats["streams_monitored"] == 1
            assert stats["checks_performed"] >= 1  # initial check
        finally:
            await monitor.stop()
