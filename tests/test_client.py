"""Tests for SatoriNostr client.

Requires local strfry relay at ws://localhost:7777.
Start with: cd src/satorilib/relay && docker compose up -d

Tests are organized bottom-up:
1. Construction and key parsing (no relay)
2. State management (no relay)
3. Lifecycle start/stop (relay)
4. Provider APIs - announce, publish (relay)
5. Subscriber APIs - discover, subscribe (relay)
6. End-to-end: provider publishes, subscriber receives (relay)
"""
import asyncio
import time
import json
import pytest
import pytest_asyncio

from nostr_sdk import Keys, SecretKey

from satorilib.satori_nostr import (
    SatoriNostr,
    SatoriNostrConfig,
    DatastreamMetadata,
    DatastreamObservation,
    SubscriptionAnnouncement,
    PaymentNotification,
    InboundObservation,
    InboundPayment,
    SubscriberState,
)
from satorilib.satori_nostr.encryption import encrypt_observation, encrypt_payment
from tests.mini_relay import MiniRelay


# =========================================================================
# Helpers
# =========================================================================

def make_config(keys: Keys, relay_url: str) -> SatoriNostrConfig:
    return SatoriNostrConfig(
        keys=keys.secret_key().to_hex(),
        relay_urls=[relay_url],
    )


def make_free_metadata(provider_keys: Keys, **overrides) -> DatastreamMetadata:
    defaults = dict(
        stream_name="test-stream",
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


def make_paid_metadata(provider_keys: Keys, **overrides) -> DatastreamMetadata:
    defaults = dict(
        stream_name="paid-stream",
        nostr_pubkey=provider_keys.public_key().to_hex(),
        name="Paid Stream",
        description="A paid test stream",
        encrypted=True,
        price_per_obs=100,
        created_at=int(time.time()),
        cadence_seconds=60,
        tags=["test", "paid"],
        metadata=None,
    )
    defaults.update(overrides)
    return DatastreamMetadata(**defaults)


def make_observation(stream_name: str = "test-stream", seq_num: int = 1) -> DatastreamObservation:
    return DatastreamObservation(
        stream_name=stream_name,
        timestamp=int(time.time()),
        value={"price": 45000.0, "seq": seq_num},
        seq_num=seq_num,
    )


# =========================================================================
# 1. Construction and key parsing
# =========================================================================

class TestClientConstruction:
    def test_create_with_hex_key(self, provider_keys, relay_url):
        config = make_config(provider_keys, relay_url)
        client = SatoriNostr(config)
        assert client.pubkey() == provider_keys.public_key().to_hex()

    def test_create_with_nsec_key(self, relay_url):
        keys = Keys.generate()
        nsec = keys.secret_key().to_bech32()
        config = SatoriNostrConfig(keys=nsec, relay_urls=[relay_url])
        client = SatoriNostr(config)
        assert client.pubkey() == keys.public_key().to_hex()

    def test_not_running_initially(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        assert client.is_running() is False

    def test_initial_stats_zero(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        stats = client.get_statistics()
        assert all(v == 0 for v in stats.values())


# =========================================================================
# 2. State management (no relay needed)
# =========================================================================

class TestStateManagement:
    def test_record_subscription(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        sub_pubkey = Keys.generate().public_key().to_hex()
        client.record_subscription("test-stream", sub_pubkey)
        subs = client.get_subscribers("test-stream")
        assert sub_pubkey in subs

    def test_get_subscribers_empty(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        assert client.get_subscribers("nonexistent") == []

    def test_record_multiple_subscribers(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        sub1 = Keys.generate().public_key().to_hex()
        sub2 = Keys.generate().public_key().to_hex()
        client.record_subscription("test-stream", sub1)
        client.record_subscription("test-stream", sub2)
        subs = client.get_subscribers("test-stream")
        assert len(subs) == 2
        assert sub1 in subs
        assert sub2 in subs

    def test_record_payment(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        sub_pubkey = Keys.generate().public_key().to_hex()
        client.record_subscription("test-stream", sub_pubkey)
        client.record_payment("test-stream", sub_pubkey, seq_num=5)
        info = client.get_subscriber_info("test-stream", sub_pubkey)
        assert info is not None
        assert info.last_paid_seq == 5

    def test_record_payment_keeps_highest(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        sub_pubkey = Keys.generate().public_key().to_hex()
        client.record_subscription("test-stream", sub_pubkey)
        client.record_payment("test-stream", sub_pubkey, seq_num=5)
        client.record_payment("test-stream", sub_pubkey, seq_num=3)
        info = client.get_subscriber_info("test-stream", sub_pubkey)
        assert info.last_paid_seq == 5

    def test_record_payment_updates_higher(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        sub_pubkey = Keys.generate().public_key().to_hex()
        client.record_subscription("test-stream", sub_pubkey)
        client.record_payment("test-stream", sub_pubkey, seq_num=3)
        client.record_payment("test-stream", sub_pubkey, seq_num=7)
        info = client.get_subscriber_info("test-stream", sub_pubkey)
        assert info.last_paid_seq == 7

    def test_get_subscriber_info_nonexistent(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        assert client.get_subscriber_info("x", "y") is None

    def test_get_all_subscribers_info(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        sub1 = Keys.generate().public_key().to_hex()
        sub2 = Keys.generate().public_key().to_hex()
        client.record_subscription("test-stream", sub1, payment_channel="ln1")
        client.record_subscription("test-stream", sub2)
        all_info = client.get_all_subscribers_info("test-stream")
        assert len(all_info) == 2
        assert all_info[sub1].payment_channel == "ln1"
        assert all_info[sub2].payment_channel is None

    def test_list_announced_streams_empty(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        assert client.list_announced_streams() == []


# =========================================================================
# 3. Lifecycle - start/stop (relay needed)
# =========================================================================

class TestLifecycle:
    @pytest.mark.asyncio
    async def test_start_stop(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        await client.start()
        assert client.is_running() is True
        await client.stop()
        assert client.is_running() is False

    @pytest.mark.asyncio
    async def test_double_start_raises(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        await client.start()
        try:
            with pytest.raises(RuntimeError):
                await client.start()
        finally:
            await client.stop()

    @pytest.mark.asyncio
    async def test_stop_without_start_raises(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        with pytest.raises(RuntimeError):
            await client.stop()


# =========================================================================
# 4. Provider APIs (relay needed)
# =========================================================================

class TestProviderAPIs:
    @pytest.mark.asyncio
    async def test_announce_datastream(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        await client.start()
        try:
            metadata = make_free_metadata(provider_keys)
            event_id = await client.announce_datastream(metadata)
            assert isinstance(event_id, str)
            assert len(event_id) == 64  # hex event id
            # Should be tracked locally
            announced = client.list_announced_streams()
            assert len(announced) == 1
            assert announced[0].stream_name == "test-stream"
        finally:
            await client.stop()

    @pytest.mark.asyncio
    async def test_announce_not_running_raises(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        metadata = make_free_metadata(provider_keys)
        with pytest.raises(RuntimeError):
            await client.announce_datastream(metadata)

    @pytest.mark.asyncio
    async def test_publish_free_observation(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        await client.start()
        try:
            metadata = make_free_metadata(provider_keys)
            obs = make_observation()
            event_ids = await client.publish_observation(obs, metadata)
            assert len(event_ids) == 1  # single broadcast event
            assert len(event_ids[0]) == 64
            assert client.get_statistics()["observations_sent"] == 1
        finally:
            await client.stop()

    @pytest.mark.asyncio
    async def test_publish_paid_no_subscribers(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        await client.start()
        try:
            metadata = make_paid_metadata(provider_keys)
            obs = make_observation(stream_name="paid-stream")
            event_ids = await client.publish_observation(obs, metadata)
            assert event_ids == []  # no subscribers = no events sent
        finally:
            await client.stop()

    @pytest.mark.asyncio
    async def test_publish_paid_with_subscriber(self, provider_keys, subscriber_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        await client.start()
        try:
            sub_pubkey = subscriber_keys.public_key().to_hex()
            client.record_subscription("paid-stream", sub_pubkey)
            client.record_payment("paid-stream", sub_pubkey, seq_num=10)

            metadata = make_paid_metadata(provider_keys)
            obs = make_observation(stream_name="paid-stream", seq_num=5)
            event_ids = await client.publish_observation(obs, metadata)
            assert len(event_ids) == 1  # sent to one subscriber
        finally:
            await client.stop()

    @pytest.mark.asyncio
    async def test_publish_paid_subscriber_not_paid_enough(self, provider_keys, subscriber_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        await client.start()
        try:
            sub_pubkey = subscriber_keys.public_key().to_hex()
            client.record_subscription("paid-stream", sub_pubkey)
            client.record_payment("paid-stream", sub_pubkey, seq_num=3)

            metadata = make_paid_metadata(provider_keys)
            # seq_num 5 > last_paid_seq 3, so subscriber shouldn't get it
            obs = make_observation(stream_name="paid-stream", seq_num=5)
            event_ids = await client.publish_observation(obs, metadata)
            assert event_ids == []
        finally:
            await client.stop()


# =========================================================================
# 5. Subscriber APIs (relay needed)
# =========================================================================

class TestSubscriberAPIs:
    @pytest.mark.asyncio
    async def test_discover_datastreams(self, provider_keys, subscriber_keys, relay_url):
        """Provider announces, subscriber discovers."""
        provider = SatoriNostr(make_config(provider_keys, relay_url))
        subscriber = SatoriNostr(make_config(subscriber_keys, relay_url))
        await provider.start()
        await subscriber.start()
        try:
            # Provider announces
            metadata = make_free_metadata(
                provider_keys,
                stream_name=f"discover-test-{int(time.time())}",
            )
            await provider.announce_datastream(metadata)
            # Small delay for relay propagation
            await asyncio.sleep(0.5)

            # Subscriber discovers
            streams = await subscriber.discover_datastreams()
            stream_names = [s.stream_name for s in streams]
            assert metadata.stream_name in stream_names
        finally:
            await provider.stop()
            await subscriber.stop()

    @pytest.mark.asyncio
    async def test_subscribe_datastream(self, subscriber_keys, relay_url):
        subscriber = SatoriNostr(make_config(subscriber_keys, relay_url))
        await subscriber.start()
        try:
            provider_pubkey = Keys.generate().public_key().to_hex()
            event_id = await subscriber.subscribe_datastream(
                "test-stream", provider_pubkey)
            assert isinstance(event_id, str)
            assert len(event_id) == 64
            assert subscriber.get_statistics()["subscriptions_announced"] == 1
        finally:
            await subscriber.stop()

    @pytest.mark.asyncio
    async def test_subscribe_not_running_raises(self, subscriber_keys, relay_url):
        subscriber = SatoriNostr(make_config(subscriber_keys, relay_url))
        with pytest.raises(RuntimeError):
            await subscriber.subscribe_datastream("x", "y")

    @pytest.mark.asyncio
    async def test_send_payment(self, subscriber_keys, relay_url):
        subscriber = SatoriNostr(make_config(subscriber_keys, relay_url))
        await subscriber.start()
        try:
            provider_pubkey = Keys.generate().public_key().to_hex()
            event_id = await subscriber.send_payment(
                provider_pubkey, "test-stream", seq_num=1, amount_sats=100)
            assert isinstance(event_id, str)
            assert len(event_id) == 64
            assert subscriber.get_statistics()["payments_sent"] == 1
        finally:
            await subscriber.stop()

    @pytest.mark.asyncio
    async def test_unsubscribe_datastream(self, subscriber_keys, relay_url):
        subscriber = SatoriNostr(make_config(subscriber_keys, relay_url))
        await subscriber.start()
        try:
            provider_pubkey = Keys.generate().public_key().to_hex()
            event_id = await subscriber.unsubscribe_datastream(
                "test-stream", provider_pubkey)
            assert isinstance(event_id, str)
            assert len(event_id) == 64
        finally:
            await subscriber.stop()

    @pytest.mark.asyncio
    async def test_get_datastream(self, provider_keys, subscriber_keys, relay_url):
        provider = SatoriNostr(make_config(provider_keys, relay_url))
        subscriber = SatoriNostr(make_config(subscriber_keys, relay_url))
        await provider.start()
        await subscriber.start()
        try:
            stream_name = f"lookup-test-{int(time.time())}"
            metadata = make_free_metadata(provider_keys, stream_name=stream_name)
            await provider.announce_datastream(metadata)
            await asyncio.sleep(0.5)

            result = await subscriber.get_datastream(stream_name)
            assert result is not None
            assert result.stream_name == stream_name
            assert result.name == "Test Stream"
        finally:
            await provider.stop()
            await subscriber.stop()

    @pytest.mark.asyncio
    async def test_get_datastream_not_found(self, subscriber_keys, relay_url):
        subscriber = SatoriNostr(make_config(subscriber_keys, relay_url))
        await subscriber.start()
        try:
            result = await subscriber.get_datastream("nonexistent-stream-xyz")
            assert result is None
        finally:
            await subscriber.stop()

    @pytest.mark.asyncio
    async def test_discover_not_running_raises(self, subscriber_keys, relay_url):
        subscriber = SatoriNostr(make_config(subscriber_keys, relay_url))
        with pytest.raises(RuntimeError):
            await subscriber.discover_datastreams()


# =========================================================================
# 6. End-to-end: provider publishes, subscriber receives
# =========================================================================

class TestEndToEnd:
    @pytest.mark.asyncio
    async def test_free_stream_e2e(self, provider_keys, subscriber_keys, relay_url):
        """Full flow: announce -> subscribe -> publish -> receive."""
        provider = SatoriNostr(make_config(provider_keys, relay_url))
        subscriber = SatoriNostr(make_config(subscriber_keys, relay_url))

        stream_name = f"e2e-free-{int(time.time())}"

        await provider.start()
        await subscriber.start()
        try:
            # 1. Provider announces stream
            metadata = make_free_metadata(provider_keys, stream_name=stream_name)
            await provider.announce_datastream(metadata)

            # 2. Subscriber subscribes
            await subscriber.subscribe_datastream(
                stream_name, provider_keys.public_key().to_hex())

            # Give relay time to process subscription
            await asyncio.sleep(1)

            # 3. Provider publishes observation
            obs = DatastreamObservation(
                stream_name=stream_name,
                timestamp=int(time.time()),
                value={"price": 42000.0},
                seq_num=1,
            )
            await provider.publish_observation(obs, metadata)

            # 4. Subscriber receives observation
            received = None
            async for inbound in subscriber.observations():
                if inbound.stream_name == stream_name:
                    received = inbound
                    break

            assert received is not None
            assert received.observation.value == {"price": 42000.0}
            assert received.observation.seq_num == 1
            assert received.nostr_pubkey == provider_keys.public_key().to_hex()

        finally:
            await provider.stop()
            await subscriber.stop()


# =========================================================================
# 7. Not-running guards
# =========================================================================

class TestNotRunningGuards:
    @pytest.mark.asyncio
    async def test_publish_observation_not_running(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        metadata = make_free_metadata(provider_keys)
        obs = make_observation()
        with pytest.raises(RuntimeError):
            await client.publish_observation(obs, metadata)

    @pytest.mark.asyncio
    async def test_send_payment_not_running(self, subscriber_keys, relay_url):
        client = SatoriNostr(make_config(subscriber_keys, relay_url))
        with pytest.raises(RuntimeError):
            await client.send_payment("aa" * 32, "s", seq_num=1, amount_sats=10)

    @pytest.mark.asyncio
    async def test_get_datastream_not_running(self, subscriber_keys, relay_url):
        client = SatoriNostr(make_config(subscriber_keys, relay_url))
        with pytest.raises(RuntimeError):
            await client.get_datastream("x")

    @pytest.mark.asyncio
    async def test_unsubscribe_not_running(self, subscriber_keys, relay_url):
        client = SatoriNostr(make_config(subscriber_keys, relay_url))
        with pytest.raises(RuntimeError):
            await client.unsubscribe_datastream("x", "aa" * 32)

    @pytest.mark.asyncio
    async def test_get_last_observation_time_not_running(self, subscriber_keys, relay_url):
        client = SatoriNostr(make_config(subscriber_keys, relay_url))
        with pytest.raises(RuntimeError):
            await client.get_last_observation_time("x")


# =========================================================================
# 8. Encrypted broadcast (free + encrypted stream)
# =========================================================================

class TestEncryptedBroadcast:
    @pytest.mark.asyncio
    async def test_publish_encrypted_free_stream(self, provider_keys, relay_url):
        """Free stream with encrypted=True encrypts with provider's own key."""
        client = SatoriNostr(make_config(provider_keys, relay_url))
        await client.start()
        try:
            metadata = make_free_metadata(
                provider_keys, encrypted=True, stream_name="enc-free")
            obs = make_observation(stream_name="enc-free")
            event_ids = await client.publish_observation(obs, metadata)
            assert len(event_ids) == 1
            assert client.get_statistics()["observations_sent"] == 1
        finally:
            await client.stop()


# =========================================================================
# 9. Tag-filtered discovery
# =========================================================================

class TestTagDiscovery:
    @pytest.mark.asyncio
    async def test_discover_with_tags(self, provider_keys, subscriber_keys, relay_url):
        provider = SatoriNostr(make_config(provider_keys, relay_url))
        subscriber = SatoriNostr(make_config(subscriber_keys, relay_url))
        await provider.start()
        await subscriber.start()
        try:
            metadata = make_free_metadata(
                provider_keys,
                stream_name=f"tagged-{int(time.time())}",
                tags=["bitcoin", "price"],
            )
            await provider.announce_datastream(metadata)
            await asyncio.sleep(0.5)

            streams = await subscriber.discover_datastreams(tags=["bitcoin"])
            names = [s.stream_name for s in streams]
            assert metadata.stream_name in names
        finally:
            await provider.stop()
            await subscriber.stop()


# =========================================================================
# 10. Active datastream discovery
# =========================================================================

class TestActiveDiscovery:
    @pytest.mark.asyncio
    async def test_discover_active_with_recent_obs(self, provider_keys, subscriber_keys, relay_url):
        """Stream with recent observation should be discovered as active."""
        provider = SatoriNostr(make_config(provider_keys, relay_url))
        subscriber = SatoriNostr(make_config(subscriber_keys, relay_url))
        await provider.start()
        await subscriber.start()
        try:
            stream_name = f"active-disc-{int(time.time())}"
            metadata = make_free_metadata(
                provider_keys, stream_name=stream_name, cadence_seconds=3600)
            await provider.announce_datastream(metadata)

            obs = make_observation(stream_name=stream_name)
            await provider.publish_observation(obs, metadata)
            await asyncio.sleep(0.5)

            active = await subscriber.discover_active_datastreams()
            names = [s.stream_name for s in active]
            assert stream_name in names
        finally:
            await provider.stop()
            await subscriber.stop()

    @pytest.mark.asyncio
    async def test_discover_active_no_obs_excluded(self, provider_keys, subscriber_keys, relay_url):
        """Stream with no observations should NOT appear in active discovery."""
        provider = SatoriNostr(make_config(provider_keys, relay_url))
        subscriber = SatoriNostr(make_config(subscriber_keys, relay_url))
        await provider.start()
        await subscriber.start()
        try:
            stream_name = f"inactive-disc-{int(time.time())}"
            metadata = make_free_metadata(
                provider_keys, stream_name=stream_name)
            await provider.announce_datastream(metadata)
            await asyncio.sleep(0.5)

            active = await subscriber.discover_active_datastreams()
            names = [s.stream_name for s in active]
            assert stream_name not in names
        finally:
            await provider.stop()
            await subscriber.stop()


# =========================================================================
# 11. Payment event handling (E2E)
# =========================================================================

class TestPaymentE2E:
    @pytest.mark.asyncio
    async def test_send_and_receive_payment(self, provider_keys, subscriber_keys, relay_url):
        """Subscriber sends payment, provider receives it."""
        provider = SatoriNostr(make_config(provider_keys, relay_url))
        subscriber = SatoriNostr(make_config(subscriber_keys, relay_url))

        await provider.start()
        await subscriber.start()
        try:
            stream_name = f"pay-e2e-{int(time.time())}"

            # Provider sets up stream
            metadata = make_paid_metadata(
                provider_keys, stream_name=stream_name)
            await provider.announce_datastream(metadata)

            # Record subscriber so provider knows them
            sub_pub = subscriber_keys.public_key().to_hex()
            provider.record_subscription(stream_name, sub_pub)

            await asyncio.sleep(0.5)

            # Subscriber sends payment
            await subscriber.send_payment(
                provider_pubkey=provider_keys.public_key().to_hex(),
                stream_name=stream_name,
                seq_num=1,
                amount_sats=100,
            )

            # Provider receives payment
            received = None
            async for inbound in provider.payments():
                received = inbound
                break

            assert received is not None
            assert received.payment.stream_name == stream_name
            assert received.payment.amount_sats == 100
            assert received.payment.seq_num == 1
            assert received.payment.from_pubkey == sub_pub

        finally:
            await provider.stop()
            await subscriber.stop()


# =========================================================================
# 12. Subscription event handling (provider receives subscription)
# =========================================================================

class TestSubscriptionEventE2E:
    @pytest.mark.asyncio
    async def test_provider_receives_subscription(self, provider_keys, subscriber_keys, relay_url):
        """When subscriber subscribes, provider's _handle_subscription_event records it."""
        provider = SatoriNostr(make_config(provider_keys, relay_url))
        subscriber = SatoriNostr(make_config(subscriber_keys, relay_url))

        await provider.start()
        await subscriber.start()
        try:
            stream_name = f"sub-evt-{int(time.time())}"

            # Provider announces stream
            metadata = make_free_metadata(
                provider_keys, stream_name=stream_name)
            await provider.announce_datastream(metadata)
            await asyncio.sleep(0.3)

            # Subscriber subscribes
            await subscriber.subscribe_datastream(
                stream_name, provider_keys.public_key().to_hex())

            # Wait for event to propagate
            await asyncio.sleep(1.0)

            # Provider should have recorded the subscription
            subs = provider.get_subscribers(stream_name)
            assert subscriber_keys.public_key().to_hex() in subs

        finally:
            await provider.stop()
            await subscriber.stop()


# =========================================================================
# 13. Observations iterator timeout (coverage for L674,681)
# =========================================================================

class TestObservationsIterator:
    @pytest.mark.asyncio
    async def test_observations_not_running_raises(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        with pytest.raises(RuntimeError):
            async for _ in client.observations():
                pass

    @pytest.mark.asyncio
    async def test_observations_timeout_continues(self, provider_keys, relay_url):
        """When no observations arrive, iterator times out and continues."""
        client = SatoriNostr(make_config(provider_keys, relay_url))
        await client.start()
        try:
            # Stop client after a short delay to break the loop
            async def stop_soon():
                await asyncio.sleep(1.5)
                client._running = False

            asyncio.create_task(stop_soon())

            count = 0
            async for _ in client.observations():
                count += 1

            # Should have exited cleanly with 0 observations
            assert count == 0
        finally:
            if client.is_running():
                await client.stop()


# =========================================================================
# 14. Payments iterator (coverage for L692-700)
# =========================================================================

class TestPaymentsIterator:
    @pytest.mark.asyncio
    async def test_payments_not_running_raises(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(provider_keys, relay_url))
        with pytest.raises(RuntimeError):
            async for _ in client.payments():
                pass

    @pytest.mark.asyncio
    async def test_payments_timeout_continues(self, provider_keys, relay_url):
        """When no payments arrive, iterator times out and continues."""
        client = SatoriNostr(make_config(provider_keys, relay_url))
        await client.start()
        try:
            async def stop_soon():
                await asyncio.sleep(1.5)
                client._running = False

            asyncio.create_task(stop_soon())

            count = 0
            async for _ in client.payments():
                count += 1

            assert count == 0
        finally:
            if client.is_running():
                await client.stop()
