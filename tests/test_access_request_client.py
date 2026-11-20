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
