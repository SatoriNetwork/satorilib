"""Tests for bounty client methods (Phase 1).

Covers: announce_bounty, close_bounty, discover_bounties.
Requires the in-process MiniRelay (no external relay or Docker needed).
"""
import time
import pytest
import pytest_asyncio

from nostr_sdk import Keys

from satorilib.satori_nostr import SatoriNostr, SatoriNostrConfig
from satorilib.satori_nostr.models import BountyAnnouncement
from tests.mini_relay import MiniRelay


def make_config(keys: Keys, relay_url: str) -> SatoriNostrConfig:
    return SatoriNostrConfig(
        keys=keys.secret_key().to_hex(),
        relay_urls=[relay_url],
    )


def make_bounty(host_keys: Keys, provider_keys: Keys, **overrides) -> BountyAnnouncement:
    defaults = dict(
        stream_name='btc-price-usd',
        stream_provider_pubkey=provider_keys.public_key().to_hex(),
        host_pubkey=host_keys.public_key().to_hex(),
        pay_per_obs_sats=300,
        paid_predictors=3,
        competing_predictors=5,
        scoring_metric='mae',
        scoring_params={},
        horizon=1,
        active=True,
        timestamp=int(time.time()),
    )
    defaults.update(overrides)
    return BountyAnnouncement(**defaults)


class TestAnnounceBounty:

    @pytest.mark.asyncio
    async def test_returns_event_id(self, provider_keys, relay_url):
        host_keys = Keys.generate()
        client = SatoriNostr(make_config(host_keys, relay_url))
        await client.start()
        try:
            bounty = make_bounty(host_keys, provider_keys)
            event_id = await client.announce_bounty(bounty)
            assert isinstance(event_id, str)
            assert len(event_id) == 64
        finally:
            await client.stop()

    @pytest.mark.asyncio
    async def test_not_running_raises(self, provider_keys, relay_url):
        host_keys = Keys.generate()
        client = SatoriNostr(make_config(host_keys, relay_url))
        bounty = make_bounty(host_keys, provider_keys)
        with pytest.raises(RuntimeError):
            await client.announce_bounty(bounty)


class TestCloseBounty:

    @pytest.mark.asyncio
    async def test_close_publishes_inactive(self, provider_keys, relay_url):
        host_keys = Keys.generate()
        client = SatoriNostr(make_config(host_keys, relay_url))
        await client.start()
        try:
            bounty = make_bounty(host_keys, provider_keys)
            await client.announce_bounty(bounty)
            event_id = await client.close_bounty(bounty)
            assert isinstance(event_id, str)
            assert len(event_id) == 64
        finally:
            await client.stop()

    @pytest.mark.asyncio
    async def test_not_running_raises(self, provider_keys, relay_url):
        host_keys = Keys.generate()
        client = SatoriNostr(make_config(host_keys, relay_url))
        bounty = make_bounty(host_keys, provider_keys)
        with pytest.raises(RuntimeError):
            await client.close_bounty(bounty)


class TestDiscoverBounties:

    @pytest.mark.asyncio
    async def test_empty_when_none_announced(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(Keys.generate(), relay_url))
        await client.start()
        try:
            results = await client.discover_bounties()
            assert results == []
        finally:
            await client.stop()

    @pytest.mark.asyncio
    async def test_discovers_announced_bounty(self, provider_keys, relay_url):
        host_keys = Keys.generate()
        host_client = SatoriNostr(make_config(host_keys, relay_url))
        searcher_client = SatoriNostr(make_config(Keys.generate(), relay_url))
        await host_client.start()
        await searcher_client.start()
        try:
            bounty = make_bounty(host_keys, provider_keys)
            await host_client.announce_bounty(bounty)
            results = await searcher_client.discover_bounties()
            assert len(results) == 1
            assert results[0].stream_name == 'btc-price-usd'
            assert results[0].pay_per_obs_sats == 300
            assert results[0].active is True
        finally:
            await host_client.stop()
            await searcher_client.stop()

    @pytest.mark.asyncio
    async def test_closed_bounty_not_returned(self, provider_keys, relay_url):
        host_keys = Keys.generate()
        host_client = SatoriNostr(make_config(host_keys, relay_url))
        searcher_client = SatoriNostr(make_config(Keys.generate(), relay_url))
        await host_client.start()
        await searcher_client.start()
        try:
            bounty = make_bounty(host_keys, provider_keys)
            await host_client.announce_bounty(bounty)
            await host_client.close_bounty(bounty)
            results = await searcher_client.discover_bounties(active_only=True)
            assert results == []
        finally:
            await host_client.stop()
            await searcher_client.stop()

    @pytest.mark.asyncio
    async def test_filter_by_stream_name(self, provider_keys, relay_url):
        host_keys = Keys.generate()
        client = SatoriNostr(make_config(host_keys, relay_url))
        searcher = SatoriNostr(make_config(Keys.generate(), relay_url))
        await client.start()
        await searcher.start()
        try:
            btc = make_bounty(host_keys, provider_keys, stream_name='btc-price')
            eth = make_bounty(host_keys, provider_keys, stream_name='eth-price')
            await client.announce_bounty(btc)
            await client.announce_bounty(eth)
            results = await searcher.discover_bounties(stream_name='btc-price')
            assert len(results) == 1
            assert results[0].stream_name == 'btc-price'
        finally:
            await client.stop()
            await searcher.stop()
