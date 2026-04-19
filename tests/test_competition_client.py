"""Tests for competition client methods (Phase 1).

Covers: announce_competition, close_competition, discover_competitions.
Requires the in-process MiniRelay (no external relay or Docker needed).
"""
import time
import pytest
import pytest_asyncio

from nostr_sdk import Keys

from satorilib.satori_nostr import SatoriNostr, SatoriNostrConfig
from satorilib.satori_nostr.models import CompetitionAnnouncement
from tests.mini_relay import MiniRelay


def make_config(keys: Keys, relay_url: str) -> SatoriNostrConfig:
    return SatoriNostrConfig(
        keys=keys.secret_key().to_hex(),
        relay_urls=[relay_url],
    )


def make_competition(host_keys: Keys, provider_keys: Keys, **overrides) -> CompetitionAnnouncement:
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
    return CompetitionAnnouncement(**defaults)


class TestAnnounceCompetition:

    @pytest.mark.asyncio
    async def test_returns_event_id(self, provider_keys, relay_url):
        host_keys = Keys.generate()
        client = SatoriNostr(make_config(host_keys, relay_url))
        await client.start()
        try:
            competition = make_competition(host_keys, provider_keys)
            event_id = await client.announce_competition(competition)
            assert isinstance(event_id, str)
            assert len(event_id) == 64
        finally:
            await client.stop()

    @pytest.mark.asyncio
    async def test_not_running_raises(self, provider_keys, relay_url):
        host_keys = Keys.generate()
        client = SatoriNostr(make_config(host_keys, relay_url))
        competition = make_competition(host_keys, provider_keys)
        with pytest.raises(RuntimeError):
            await client.announce_competition(competition)


class TestCloseCompetition:

    @pytest.mark.asyncio
    async def test_close_publishes_inactive(self, provider_keys, relay_url):
        host_keys = Keys.generate()
        client = SatoriNostr(make_config(host_keys, relay_url))
        await client.start()
        try:
            competition = make_competition(host_keys, provider_keys)
            await client.announce_competition(competition)
            event_id = await client.close_competition(competition)
            assert isinstance(event_id, str)
            assert len(event_id) == 64
        finally:
            await client.stop()

    @pytest.mark.asyncio
    async def test_not_running_raises(self, provider_keys, relay_url):
        host_keys = Keys.generate()
        client = SatoriNostr(make_config(host_keys, relay_url))
        competition = make_competition(host_keys, provider_keys)
        with pytest.raises(RuntimeError):
            await client.close_competition(competition)


class TestDiscoverCompetitions:

    @pytest.mark.asyncio
    async def test_empty_when_none_announced(self, provider_keys, relay_url):
        client = SatoriNostr(make_config(Keys.generate(), relay_url))
        await client.start()
        try:
            results = await client.discover_competitions()
            assert results == []
        finally:
            await client.stop()

    @pytest.mark.asyncio
    async def test_discovers_announced_competition(self, provider_keys, relay_url):
        host_keys = Keys.generate()
        host_client = SatoriNostr(make_config(host_keys, relay_url))
        searcher_client = SatoriNostr(make_config(Keys.generate(), relay_url))
        await host_client.start()
        await searcher_client.start()
        try:
            competition = make_competition(host_keys, provider_keys)
            await host_client.announce_competition(competition)
            results = await searcher_client.discover_competitions()
            assert len(results) == 1
            assert results[0].stream_name == 'btc-price-usd'
            assert results[0].pay_per_obs_sats == 300
            assert results[0].active is True
        finally:
            await host_client.stop()
            await searcher_client.stop()

    @pytest.mark.asyncio
    async def test_closed_competition_not_returned(self, provider_keys, relay_url):
        host_keys = Keys.generate()
        host_client = SatoriNostr(make_config(host_keys, relay_url))
        searcher_client = SatoriNostr(make_config(Keys.generate(), relay_url))
        await host_client.start()
        await searcher_client.start()
        try:
            competition = make_competition(host_keys, provider_keys)
            await host_client.announce_competition(competition)
            await host_client.close_competition(competition)
            results = await searcher_client.discover_competitions(active_only=True)
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
            btc = make_competition(host_keys, provider_keys, stream_name='btc-price')
            eth = make_competition(host_keys, provider_keys, stream_name='eth-price')
            await client.announce_competition(btc)
            await client.announce_competition(eth)
            results = await searcher.discover_competitions(stream_name='btc-price')
            assert len(results) == 1
            assert results[0].stream_name == 'btc-price'
        finally:
            await client.stop()
            await searcher.stop()
