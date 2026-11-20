"""Tests for prediction submission client methods (Phase 2).

Covers: submit_prediction, incoming_predictions.
"""
import asyncio
import time
import pytest
import pytest_asyncio

from nostr_sdk import EventBuilder, Keys, Kind, Tag

from satorilib.satori_nostr import SatoriNostr, SatoriNostrConfig
from satorilib.satori_nostr.encryption import encrypt_json
from satorilib.satori_nostr.models import KIND_PREDICTION, PredictionSubmission


def make_config(keys: Keys, relay_url: str) -> SatoriNostrConfig:
    return SatoriNostrConfig(
        keys=keys.secret_key().to_hex(),
        relay_urls=[relay_url],
    )


class TestSubmitPrediction:

    @pytest.mark.asyncio
    async def test_returns_event_id(self, provider_keys, relay_url):
        predictor_keys = Keys.generate()
        host_keys = Keys.generate()
        predictor = SatoriNostr(make_config(predictor_keys, relay_url))
        await predictor.start()
        try:
            event_id = await predictor.submit_prediction(
                stream_name='btc-price-usd',
                stream_provider_pubkey=provider_keys.public_key().to_hex(),
                host_pubkey=host_keys.public_key().to_hex(),
                seq_num=42,
                predicted_value=67450.25,
                predictor_alias_pubkey='predictor_wallet_01',
            )
            assert isinstance(event_id, str)
            assert len(event_id) == 64
        finally:
            await predictor.stop()

    @pytest.mark.asyncio
    async def test_not_running_raises(self, provider_keys, relay_url):
        predictor = SatoriNostr(make_config(Keys.generate(), relay_url))
        with pytest.raises(RuntimeError):
            await predictor.submit_prediction(
                stream_name='btc-price-usd',
                stream_provider_pubkey=provider_keys.public_key().to_hex(),
                host_pubkey=Keys.generate().public_key().to_hex(),
                seq_num=1,
                predicted_value=1.0,
                predictor_alias_pubkey='predictor_wallet_01',
            )

    @pytest.mark.asyncio
    async def test_a_slot_a_bounty(self, provider_keys, relay):
        """A relay keeps one prediction a (predictor, d). Each bounty is a
        slot: a predictor's to two hosts are both kept, whatever second they
        are sent in, and its next to a host takes the place of its last."""
        predictor = SatoriNostr(make_config(Keys.generate(), relay.url))
        hosts = [Keys.generate().public_key().to_hex() for _ in range(2)]
        await predictor.start()
        try:
            for seq_num, host in enumerate(hosts + hosts[:1]):
                await predictor.submit_prediction(
                    stream_name='btc-price-usd',
                    stream_provider_pubkey=provider_keys.public_key().to_hex(),
                    host_pubkey=host,
                    seq_num=seq_num,
                    predicted_value=1.0,
                    predictor_alias_pubkey='predictor_wallet_01',
                )
            kept = [tag[1] for event in relay.events
                    if event['kind'] == KIND_PREDICTION
                    for tag in event['tags'] if tag[0] == 'p']
            assert sorted(kept) == sorted(hosts)
        finally:
            await predictor.stop()


class TestIncomingPredictions:

    @pytest.mark.asyncio
    async def test_host_receives_prediction(self, provider_keys, relay_url):
        predictor_keys = Keys.generate()
        host_keys = Keys.generate()
        predictor = SatoriNostr(make_config(predictor_keys, relay_url))
        host = SatoriNostr(make_config(host_keys, relay_url))
        await predictor.start()
        await host.start()
        try:
            await predictor.submit_prediction(
                stream_name='btc-price-usd',
                stream_provider_pubkey=provider_keys.public_key().to_hex(),
                host_pubkey=host_keys.public_key().to_hex(),
                seq_num=42,
                predicted_value=67450.25,
                predictor_alias_pubkey='predictor_wallet_01',
            )
            # Allow event to propagate
            await asyncio.sleep(0.3)
            received = []
            async for pred in host.incoming_predictions():
                received.append(pred)
                break  # take first
            assert len(received) == 1
            p = received[0].prediction
            assert p.stream_name == 'btc-price-usd'
            assert p.seq_num == 42
            assert p.predicted_value == 67450.25
            assert p.predictor_pubkey == predictor_keys.public_key().to_hex()
        finally:
            await predictor.stop()
            await host.stop()

    @pytest.mark.asyncio
    async def test_predictor_is_the_author(self, provider_keys, relay_url):
        """A prediction that names a key other than its event's author is dropped."""
        predictor_keys = Keys.generate()
        host_keys = Keys.generate()
        predictor = SatoriNostr(make_config(predictor_keys, relay_url))
        forger = SatoriNostr(make_config(Keys.generate(), relay_url))
        host = SatoriNostr(make_config(host_keys, relay_url))
        await predictor.start()
        await forger.start()
        await host.start()
        try:
            # The forger's, in the predictor's name, stamped later, its own alias to pay
            forged = PredictionSubmission(
                stream_name='btc-price-usd',
                stream_provider_pubkey=provider_keys.public_key().to_hex(),
                predictor_pubkey=predictor_keys.public_key().to_hex(),
                predictor_alias_pubkey='forger_wallet',
                seq_num=42, predicted_value=1.0, timestamp=int(time.time()) + 60)
            await forger._send_builder(EventBuilder(Kind(KIND_PREDICTION), encrypt_json(
                forged.to_json(), host_keys.public_key(), forger._keys)).tags(
                [Tag.parse(["p", host_keys.public_key().to_hex()])]))
            await predictor.submit_prediction(
                stream_name='btc-price-usd',
                stream_provider_pubkey=provider_keys.public_key().to_hex(),
                host_pubkey=host_keys.public_key().to_hex(),
                seq_num=42,
                predicted_value=67450.25,
                predictor_alias_pubkey='predictor_wallet_01',
            )
            async for inbound in host.incoming_predictions():
                break
            await asyncio.sleep(0.3)
            assert inbound.prediction.predicted_value == 67450.25
            assert inbound.prediction.predictor_alias_pubkey == 'predictor_wallet_01'
            assert host._prediction_queue.empty()
        finally:
            await predictor.stop()
            await forger.stop()
            await host.stop()

    @pytest.mark.asyncio
    async def test_only_host_can_decrypt(self, provider_keys, relay_url):
        predictor_keys = Keys.generate()
        host_keys = Keys.generate()
        eavesdropper_keys = Keys.generate()
        predictor = SatoriNostr(make_config(predictor_keys, relay_url))
        eavesdropper = SatoriNostr(make_config(eavesdropper_keys, relay_url))
        await predictor.start()
        await eavesdropper.start()
        try:
            await predictor.submit_prediction(
                stream_name='btc-price-usd',
                stream_provider_pubkey=provider_keys.public_key().to_hex(),
                host_pubkey=host_keys.public_key().to_hex(),
                seq_num=42,
                predicted_value=99.99,
                predictor_alias_pubkey='predictor_wallet_01',
            )
            await asyncio.sleep(0.3)
            # Eavesdropper queue should be empty — can't decrypt
            assert eavesdropper._prediction_queue.empty()
        finally:
            await predictor.stop()
            await eavesdropper.stop()
