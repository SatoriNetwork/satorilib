"""Tests for ReliableSubscriber integration.

Covers: lifecycle, subscribe/unsubscribe, stream discovery,
        observation deduplication, status/stats, callbacks.
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
from satorilib.satori_nostr.integrations.reliable_subscriber import (
    ReliableSubscriber,
    SubscriptionConfig,
)
from satorilib.satori_nostr.integrations.stream_monitor import StreamHealth
from tests.mini_relay import MiniRelay


# =========================================================================
# Helpers
# =========================================================================

def make_metadata(provider_keys, stream_name="test-stream", **overrides):
    defaults = dict(
        stream_name=stream_name,
        nostr_pubkey=provider_keys.public_key().to_hex(),
        name="Test Stream",
        description="A test stream",
        encrypted=False,
        price_per_obs=0,
        created_at=int(time.time()),
        cadence_seconds=60,
        tags=["test"],
        metadata=None,
    )
    defaults.update(overrides)
    return DatastreamMetadata(**defaults)


@pytest_asyncio.fixture
async def relay():
    """Provide a running mini relay."""
    r = MiniRelay()
    await r.start()
    yield r
    await r.stop()


# =========================================================================
# SubscriptionConfig
# =========================================================================

class TestSubscriptionConfig:
    def test_defaults(self):
        c = SubscriptionConfig(
            stream_name="btc",
            provider_pubkey="aa" * 32,
        )
        assert c.auto_pay is True
        assert c.payment_channel is None

    def test_custom(self):
        c = SubscriptionConfig(
            stream_name="btc",
            provider_pubkey="aa" * 32,
            auto_pay=False,
            payment_channel="lnbc1...",
        )
        assert c.auto_pay is False
        assert c.payment_channel == "lnbc1..."


# =========================================================================
# Lifecycle
# =========================================================================

class TestLifecycle:
    @pytest.mark.asyncio
    async def test_start_stop(self, relay):
        keys = Keys.generate()
        sub = ReliableSubscriber(
            keys=keys.secret_key().to_hex(),
            relay_urls=[relay.url],
        )
        await sub.start()
        assert sub._running is True
        await sub.stop()
        assert sub._running is False

    @pytest.mark.asyncio
    async def test_double_start_raises(self, relay):
        keys = Keys.generate()
        sub = ReliableSubscriber(
            keys=keys.secret_key().to_hex(),
            relay_urls=[relay.url],
        )
        await sub.start()
        try:
            with pytest.raises(RuntimeError):
                await sub.start()
        finally:
            await sub.stop()

    @pytest.mark.asyncio
    async def test_stop_when_not_running(self, relay):
        keys = Keys.generate()
        sub = ReliableSubscriber(
            keys=keys.secret_key().to_hex(),
            relay_urls=[relay.url],
        )
        await sub.stop()  # should not raise


# =========================================================================
# Subscribe / Unsubscribe
# =========================================================================

class TestSubscription:
    @pytest.mark.asyncio
    async def test_subscribe(self, relay):
        keys = Keys.generate()
        provider_keys = Keys.generate()
        sub = ReliableSubscriber(
            keys=keys.secret_key().to_hex(),
            relay_urls=[relay.url],
        )
        await sub.start()
        try:
            await sub.subscribe(
                "test-stream",
                provider_keys.public_key().to_hex(),
            )
            config = sub.get_subscription_status("test-stream")
            assert config is not None
            assert config.stream_name == "test-stream"
            assert config.auto_pay is True
        finally:
            await sub.stop()

    @pytest.mark.asyncio
    async def test_subscribe_not_running_raises(self, relay):
        keys = Keys.generate()
        sub = ReliableSubscriber(
            keys=keys.secret_key().to_hex(),
            relay_urls=[relay.url],
        )
        with pytest.raises(RuntimeError):
            await sub.subscribe("x", "y")

    @pytest.mark.asyncio
    async def test_unsubscribe(self, relay):
        keys = Keys.generate()
        provider_keys = Keys.generate()
        sub = ReliableSubscriber(
            keys=keys.secret_key().to_hex(),
            relay_urls=[relay.url],
        )
        await sub.start()
        try:
            provider_pub = provider_keys.public_key().to_hex()
            await sub.subscribe("test-stream", provider_pub)
            await sub.unsubscribe("test-stream")
            assert sub.get_subscription_status("test-stream") is None
        finally:
            await sub.stop()

    @pytest.mark.asyncio
    async def test_unsubscribe_nonexistent_noop(self, relay):
        keys = Keys.generate()
        sub = ReliableSubscriber(
            keys=keys.secret_key().to_hex(),
            relay_urls=[relay.url],
        )
        await sub.start()
        try:
            await sub.unsubscribe("nope")  # should not raise
        finally:
            await sub.stop()

    @pytest.mark.asyncio
    async def test_subscribe_custom_options(self, relay):
        keys = Keys.generate()
        provider_keys = Keys.generate()
        sub = ReliableSubscriber(
            keys=keys.secret_key().to_hex(),
            relay_urls=[relay.url],
        )
        await sub.start()
        try:
            await sub.subscribe(
                "test-stream",
                provider_keys.public_key().to_hex(),
                auto_pay=False,
                payment_channel="lnbc1...",
            )
            config = sub.get_subscription_status("test-stream")
            assert config.auto_pay is False
            assert config.payment_channel == "lnbc1..."
        finally:
            await sub.stop()


# =========================================================================
# Stream discovery
# =========================================================================

class TestDiscovery:
    @pytest.mark.asyncio
    async def test_discover_streams(self, relay):
        provider_keys = Keys.generate()
        subscriber_keys = Keys.generate()

        # Provider announces a stream
        provider = SatoriNostr(SatoriNostrConfig(
            keys=provider_keys.secret_key().to_hex(),
            relay_urls=[relay.url],
        ))
        await provider.start()
        metadata = make_metadata(provider_keys, stream_name="disc-test")
        await provider.announce_datastream(metadata)
        await asyncio.sleep(0.3)

        # Subscriber discovers
        sub = ReliableSubscriber(
            keys=subscriber_keys.secret_key().to_hex(),
            relay_urls=[relay.url],
        )
        await sub.start()
        try:
            streams = await sub.discover_streams(active_only=False)
            names = [s.stream_name for s in streams]
            assert "disc-test" in names
        finally:
            await sub.stop()
            await provider.stop()

    @pytest.mark.asyncio
    async def test_discover_not_running_raises(self, relay):
        keys = Keys.generate()
        sub = ReliableSubscriber(
            keys=keys.secret_key().to_hex(),
            relay_urls=[relay.url],
        )
        with pytest.raises(RuntimeError):
            await sub.discover_streams()

    @pytest.mark.asyncio
    async def test_discover_deduplicates_by_uuid(self, relay):
        """Same stream announced twice should only appear once."""
        provider_keys = Keys.generate()
        subscriber_keys = Keys.generate()

        provider = SatoriNostr(SatoriNostrConfig(
            keys=provider_keys.secret_key().to_hex(),
            relay_urls=[relay.url],
        ))
        await provider.start()
        metadata = make_metadata(provider_keys, stream_name="dedup-disc")
        await provider.announce_datastream(metadata)
        await provider.announce_datastream(metadata)  # duplicate announce
        await asyncio.sleep(0.3)

        sub = ReliableSubscriber(
            keys=subscriber_keys.secret_key().to_hex(),
            relay_urls=[relay.url],
        )
        await sub.start()
        try:
            streams = await sub.discover_streams(active_only=False)
            dedup_names = [s.stream_name for s in streams
                          if s.stream_name == "dedup-disc"]
            assert len(dedup_names) == 1
        finally:
            await sub.stop()
            await provider.stop()


# =========================================================================
# Status / Statistics
# =========================================================================

class TestStatus:
    @pytest.mark.asyncio
    async def test_get_relay_status(self, relay):
        keys = Keys.generate()
        sub = ReliableSubscriber(
            keys=keys.secret_key().to_hex(),
            relay_urls=[relay.url],
        )
        await sub.start()
        try:
            status = sub.get_relay_status()
            assert relay.url in status
        finally:
            await sub.stop()

    @pytest.mark.asyncio
    async def test_get_statistics(self, relay):
        keys = Keys.generate()
        sub = ReliableSubscriber(
            keys=keys.secret_key().to_hex(),
            relay_urls=[relay.url],
        )
        await sub.start()
        try:
            stats = sub.get_statistics()
            assert "observations_received" in stats
            assert "relay" in stats
            assert "health" in stats
            assert "client" in stats
        finally:
            await sub.stop()

    @pytest.mark.asyncio
    async def test_stream_health_before_start(self, relay):
        keys = Keys.generate()
        sub = ReliableSubscriber(
            keys=keys.secret_key().to_hex(),
            relay_urls=[relay.url],
        )
        # Health monitor not started yet
        assert sub.get_stream_health("test") is None


# =========================================================================
# End-to-end: publish → receive via ReliableSubscriber
# =========================================================================

class TestE2E:
    @pytest.mark.asyncio
    async def test_receive_observation(self, relay):
        """Provider publishes, ReliableSubscriber receives."""
        provider_keys = Keys.generate()
        subscriber_keys = Keys.generate()

        # Provider
        provider = SatoriNostr(SatoriNostrConfig(
            keys=provider_keys.secret_key().to_hex(),
            relay_urls=[relay.url],
        ))
        await provider.start()

        stream_name = f"e2e-reliable-{int(time.time())}"
        metadata = make_metadata(provider_keys, stream_name=stream_name)
        await provider.announce_datastream(metadata)

        # Subscriber
        sub = ReliableSubscriber(
            keys=subscriber_keys.secret_key().to_hex(),
            relay_urls=[relay.url],
        )
        await sub.start()

        try:
            await sub.subscribe(
                stream_name,
                provider_keys.public_key().to_hex(),
            )
            await asyncio.sleep(0.5)

            # Provider publishes observation
            obs = DatastreamObservation(
                stream_name=stream_name,
                timestamp=int(time.time()),
                value={"price": 99000.0},
                seq_num=1,
            )
            await provider.publish_observation(obs, metadata)

            # Subscriber receives
            received = None
            async for inbound in sub.observations():
                if inbound.stream_name == stream_name:
                    received = inbound
                    break

            assert received is not None
            assert received.observation.value == {"price": 99000.0}
        finally:
            await sub.stop()
            await provider.stop()
