"""A public event tells whether a relay took it.

start() does not fail on a relay it cannot reach, and a send to one returned normally, so a neuron
logged "connected", "announced" and "published" for a dead relay, and listed it in its relay list.
"""
import time

import pytest

from satorilib.satori_nostr import SatoriNostr, SatoriNostrConfig
from satorilib.satori_nostr.models import DatastreamMetadata, DatastreamObservation

DEAD_RELAY = "ws://127.0.0.1:9"  # nothing listens there


def _stream(pubkey: str) -> DatastreamMetadata:
    return DatastreamMetadata(
        stream_name="s", nostr_pubkey=pubkey, name="s", description="", encrypted=False,
        price_per_obs=0, created_at=int(time.time()), cadence_seconds=3600, tags=[], metadata=None)


async def _client(keys, url) -> SatoriNostr:
    client = SatoriNostr(SatoriNostrConfig(keys=keys.secret_key().to_hex(), relay_urls=[url]))
    await client.start()
    return client


@pytest.mark.asyncio
async def test_a_reachable_relay_takes_the_events(relay_url, provider_keys):
    client = await _client(provider_keys, relay_url)
    try:
        meta = _stream(provider_keys.public_key().to_hex())
        assert await client.is_connected()
        assert await client.announce_datastream(meta)
        assert await client.publish_relay_list([relay_url])
        obs = DatastreamObservation(stream_name="s", timestamp=int(time.time()), value=1.0, seq_num=1)
        assert len(await client.publish_observation(obs, meta)) == 1
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_an_unreachable_relay_takes_none_and_says_so(provider_keys):
    client = await _client(provider_keys, DEAD_RELAY)  # starts all the same
    try:
        meta = _stream(provider_keys.public_key().to_hex())
        assert not await client.is_connected()
        assert await client.announce_datastream(meta) is None
        assert client.list_announced_streams() == []
        assert await client.publish_relay_list([DEAD_RELAY]) is None
        obs = DatastreamObservation(stream_name="s", timestamp=int(time.time()), value=1.0, seq_num=1)
        assert await client.publish_observation(obs, meta) == []
    finally:
        await client.stop()
