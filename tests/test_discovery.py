"""Relay-friendly discovery: pagination past relay limits, targeted and
batched queries, and scoped live subscriptions.

Uses a MiniRelay with max_limit=500 to mimic strfry's maxFilterLimit.
"""
import asyncio
import json
import time

import pytest
import pytest_asyncio
from nostr_sdk import EventBuilder, Keys, Kind, Tag, Timestamp

from satorilib.satori_nostr import SatoriNostr, SatoriNostrConfig
from satorilib.satori_nostr.models import (
    KIND_DATASTREAM_ANNOUNCE,
    DatastreamMetadata,
)
from tests.mini_relay import MiniRelay


@pytest_asyncio.fixture
async def capped_relay():
    relay = MiniRelay(max_limit=500)
    await relay.start()
    yield relay
    await relay.stop()


def signed_event(keys: Keys, kind: int, content: str, tags: list[list[str]],
                 created_at: int) -> dict:
    builder = (EventBuilder(Kind(kind), content)
               .tags([Tag.parse(t) for t in tags])
               .custom_created_at(Timestamp.from_secs(created_at)))
    return json.loads(builder.finalize(keys).as_json())


def announcement(keys: Keys, stream_name: str, created_at: int,
                 deleted: bool = False) -> dict:
    meta = DatastreamMetadata(
        stream_name=stream_name,
        nostr_pubkey=keys.public_key().to_hex(),
        name=stream_name,
        description="",
        encrypted=False,
        price_per_obs=0,
        created_at=created_at,
        cadence_seconds=60,
        tags=[],
        metadata=None,
    )
    tags = [["d", stream_name], ["satori", "datastream"]]
    if deleted:
        tags.append(["status", "deleted"])
    return signed_event(
        keys, KIND_DATASTREAM_ANNOUNCE, meta.to_json(), tags, created_at)


async def started_client(relay_url: str, relay: MiniRelay | None = None,
                         keys: Keys | None = None) -> SatoriNostr:
    keys = keys or Keys.generate()
    client = SatoriNostr(SatoriNostrConfig(
        keys=keys.secret_key().to_hex(), relay_urls=[relay_url]))
    before = relay.req_count if relay else 0
    await client.start()
    if relay:
        # Let the client's two default REQs land so tests can count ours.
        for _ in range(100):
            if relay.req_count >= before + 2:
                break
            await asyncio.sleep(0.02)
    return client


class TestDiscoverPagination:

    @pytest.mark.asyncio
    async def test_returns_more_than_relay_limit(self, capped_relay):
        keys = Keys.generate()
        now = int(time.time())
        # 1200 streams, several sharing each second to exercise the
        # inclusive `until` boundary.
        for i in range(1200):
            capped_relay.events.append(
                announcement(keys, f"s{i}", now - 2000 + i // 3))

        client = await started_client(capped_relay.url)
        try:
            streams = await client.discover_datastreams(limit=None)
        finally:
            await client.stop()

        assert len({s.stream_name for s in streams}) == 1200
        assert len(streams) == 1200

    @pytest.mark.asyncio
    async def test_limit_caps_total(self, capped_relay):
        keys = Keys.generate()
        now = int(time.time())
        for i in range(700):
            capped_relay.events.append(announcement(keys, f"s{i}", now - i))

        client = await started_client(capped_relay.url)
        try:
            streams = await client.discover_datastreams(limit=600)
        finally:
            await client.stop()

        assert len(streams) == 600

    @pytest.mark.asyncio
    async def test_newest_version_wins_and_tombstones_skipped(
            self, capped_relay):
        keys = Keys.generate()
        now = int(time.time())
        # Relay kept an old and a new version of the same coordinate.
        capped_relay.events.append(announcement(keys, "live", now - 100))
        capped_relay.events.append(announcement(keys, "live", now - 10))
        capped_relay.events.append(announcement(keys, "gone", now - 100))
        capped_relay.events.append(
            announcement(keys, "gone", now - 10, deleted=True))

        client = await started_client(capped_relay.url)
        try:
            streams = await client.discover_datastreams(limit=None)
        finally:
            await client.stop()

        assert [s.stream_name for s in streams] == ["live"]
        assert streams[0].created_at == now - 10


def observation(keys: Keys, stream_name: str, created_at: int,
                value: str = "1") -> dict:
    from satorilib.satori_nostr.models import (
        KIND_DATASTREAM_DATA, DatastreamObservation)
    obs = DatastreamObservation(
        stream_name=stream_name, timestamp=created_at, value=value,
        seq_num=created_at)
    return signed_event(
        keys, KIND_DATASTREAM_DATA, obs.to_json(),
        [["d", stream_name], ["stream", stream_name]], created_at)


class TestBatchedLookups:

    @pytest.mark.asyncio
    async def test_find_datastreams_returns_only_requested(self, capped_relay):
        a, b = Keys.generate(), Keys.generate()
        now = int(time.time())
        for i in range(50):
            capped_relay.events.append(announcement(a, f"s{i}", now - i))
            capped_relay.events.append(announcement(b, f"s{i}", now - i))
        capped_relay.events.append(
            announcement(a, "dead", now, deleted=True))
        a_hex, b_hex = a.public_key().to_hex(), b.public_key().to_hex()
        wanted = [(a_hex, "s1"), (a_hex, "s2"), (b_hex, "s3"),
                  (a_hex, "dead"), (a_hex, "missing")]

        client = await started_client(capped_relay.url, capped_relay)
        try:
            before = capped_relay.req_count
            found = await client.find_datastreams(wanted)
            reqs = capped_relay.req_count - before
        finally:
            await client.stop()

        assert set(found) == {(a_hex, "s1"), (a_hex, "s2"), (b_hex, "s3")}
        assert found[(b_hex, "s3")].nostr_pubkey == b_hex
        assert reqs == 1

    @pytest.mark.asyncio
    async def test_last_observation_events_batched(self, capped_relay):
        now = int(time.time())
        providers = [Keys.generate() for _ in range(250)]
        pairs = []
        for k in providers:
            for n in ("x", "y"):
                # Old and new version of each coordinate
                capped_relay.events.append(observation(k, n, now - 500))
                capped_relay.events.append(observation(k, n, now - 5, "2"))
                pairs.append((k.public_key().to_hex(), n))

        client = await started_client(capped_relay.url, capped_relay)
        try:
            before = capped_relay.req_count
            latest = await client.get_last_observation_events(pairs)
            reqs = capped_relay.req_count - before
            parsed = client.parse_observation_event(latest[pairs[0]])
        finally:
            await client.stop()

        assert set(latest) == set(pairs)
        assert all(e.created_at().as_secs() == now - 5
                   for e in latest.values())
        assert parsed.observation.value == "2"
        # 250 authors -> 250 filters -> 2 REQs (200 filters each max)
        assert reqs == 2


class TestFollowedAuthors:

    async def _drain_observations(self, client, seconds=1.0):
        got = []
        async def pull():
            async for obs in client.observations():
                got.append(obs)
        try:
            await asyncio.wait_for(pull(), timeout=seconds)
        except asyncio.TimeoutError:
            pass
        return got

    @pytest.mark.asyncio
    async def test_default_receives_all_public_observations(
            self, capped_relay):
        a, b = Keys.generate(), Keys.generate()
        client = await started_client(capped_relay.url, capped_relay)
        pa = await started_client(capped_relay.url, keys=a)
        pb = await started_client(capped_relay.url, keys=b)
        try:
            await pa._client.send_event(EventBuilder(
                Kind(34601), '').tags([]).finalize(a))  # warm-up no-op
            for keys, pub in ((a, pa), (b, pb)):
                ev = observation(keys, "s", int(time.time()))
                from nostr_sdk import Event
                await pub._client.send_event(Event.from_json(json.dumps(ev)))
            got = await self._drain_observations(client)
        finally:
            for c in (client, pa, pb):
                await c.stop()
        assert {o.nostr_pubkey for o in got} == {
            a.public_key().to_hex(), b.public_key().to_hex()}

    @pytest.mark.asyncio
    async def test_followed_authors_scopes_public_events(self, capped_relay):
        from nostr_sdk import Event
        from satorilib.satori_nostr.models import (
            KIND_CHANNEL_COMMITMENT, KIND_SUBSCRIPTION_ANNOUNCE)
        me, a, b = Keys.generate(), Keys.generate(), Keys.generate()
        a_hex, b_hex = a.public_key().to_hex(), b.public_key().to_hex()
        client = await started_client(capped_relay.url, capped_relay, keys=me)
        pub = await started_client(capped_relay.url)
        try:
            await client.set_followed_authors([a_hex])
            now = int(time.time())
            events = [
                observation(a, "s", now),
                observation(b, "s", now),
                # b tells us (p tag) it subscribed to our stream
                signed_event(b, KIND_SUBSCRIPTION_ANNOUNCE, json.dumps({
                    "subscriber_pubkey": b_hex, "stream_name": "mine",
                    "nostr_pubkey": me.public_key().to_hex(),
                    "timestamp": now, "payment_channel": None}),
                    [["d", "mine"], ["p", me.public_key().to_hex()]], now),
                # b's untagged commitment tombstone: not followed
                signed_event(b, KIND_CHANNEL_COMMITMENT, "",
                             [["d", "p2sh_b"], ["action", "claimed"]], now),
                # a's tombstone: followed
                signed_event(a, KIND_CHANNEL_COMMITMENT, "",
                             [["d", "p2sh_a"], ["action", "claimed"]], now),
            ]
            for ev in events:
                await pub._client.send_event(Event.from_json(json.dumps(ev)))
            got = await self._drain_observations(client)
            tombstones = []
            while not client._tombstone_queue.empty():
                tombstones.append(client._tombstone_queue.get_nowait())
            subscribers = client.get_subscribers("mine")
        finally:
            await client.stop()
            await pub.stop()

        assert [o.nostr_pubkey for o in got] == [a_hex]
        assert tombstones == ["p2sh_a"]
        assert b_hex in subscribers

    @pytest.mark.asyncio
    async def test_followed_authors_clear_and_resume(self, capped_relay):
        from nostr_sdk import Event
        a = Keys.generate()
        a_hex = a.public_key().to_hex()
        client = await started_client(capped_relay.url, capped_relay)
        pub = await started_client(capped_relay.url)

        async def send(name):
            ev = observation(a, name, int(time.time()))
            await pub._client.send_event(Event.from_json(json.dumps(ev)))
        try:
            await client.set_followed_authors([])
            await send("while-cleared")
            cleared = await self._drain_observations(client, 0.5)
            await client.set_followed_authors([a_hex])
            await send("after-follow")
            followed = await self._drain_observations(client)
        finally:
            await client.stop()
            await pub.stop()
        assert cleared == []
        # Following replays what the relay stored meanwhile, then live
        assert sorted(o.stream_name for o in followed) == [
            "after-follow", "while-cleared"]
