"""Tests for the access request client methods: request_access, incoming_access_requests."""
import asyncio
import time
import pytest

from nostr_sdk import EventBuilder, Keys, Kind, Tag

from satorilib.satori_nostr import SatoriNostr, SatoriNostrConfig
from satorilib.satori_nostr.encryption import encrypt_json
from satorilib.satori_nostr.models import KIND_ACCESS_REQUEST, AccessRequest


def make_config(keys: Keys, relay_url: str) -> SatoriNostrConfig:
    return SatoriNostrConfig(
        keys=keys.secret_key().to_hex(),
        relay_urls=[relay_url],
    )


class TestRequestAccess:

    @pytest.mark.asyncio
    async def test_a_slot_a_stream(self, subscriber_keys, relay):
        """A relay keeps one request a (requester, d). Each producer's stream
        is a slot: a requester's for two streams, and for two producers'
        streams of one name, are all kept, whatever second they are sent in,
        and asking again takes the place of the first ask."""
        subscriber = SatoriNostr(make_config(subscriber_keys, relay.url))
        a, b = (Keys.generate().public_key().to_hex() for _ in range(2))
        asks = [('gated-stream', a), ('gated-stream', b), ('other-stream', a)]
        await subscriber.start()
        try:
            for stream_name, producer in asks + asks[:1]:
                await subscriber.request_access(stream_name, producer)
            kept = [dict(event['tags']) for event in relay.events
                    if event['kind'] == KIND_ACCESS_REQUEST]
            assert sorted((tags['s'], tags['p']) for tags in kept) == sorted(asks)
        finally:
            await subscriber.stop()


class TestIncomingAccessRequests:

    @pytest.mark.asyncio
    async def test_requester_is_the_author(self, provider_keys, subscriber_keys, relay_url):
        """The producer hears a request from its requester; one that names a key
        other than its event's author is dropped."""
        producer = SatoriNostr(make_config(provider_keys, relay_url))
        subscriber = SatoriNostr(make_config(subscriber_keys, relay_url))
        forger = SatoriNostr(make_config(Keys.generate(), relay_url))
        await producer.start()
        await subscriber.start()
        await forger.start()
        try:
            producer_pub = provider_keys.public_key().to_hex()
            forged = AccessRequest(
                stream_name='gated-stream',
                requester_pubkey=subscriber_keys.public_key().to_hex(),
                producer_pubkey=producer_pub,
                message='forged', timestamp=int(time.time()))
            await forger._send_builder(EventBuilder(Kind(KIND_ACCESS_REQUEST), encrypt_json(
                forged.to_json(), provider_keys.public_key(), forger._keys)).tags(
                [Tag.parse(["p", producer_pub])]))
            await subscriber.request_access('gated-stream', producer_pub, message='please')
            async for inbound in producer.incoming_access_requests():
                break
            await asyncio.sleep(0.3)
            req = inbound.access_request
            assert (req.stream_name, req.requester_pubkey, req.message) == (
                'gated-stream', subscriber_keys.public_key().to_hex(), 'please')
            assert producer._access_request_queue.empty()
        finally:
            await producer.stop()
            await subscriber.stop()
            await forger.stop()
