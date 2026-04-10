"""Tests for prediction submission client methods (Phase 2).

Covers: submit_prediction, incoming_predictions.
"""
import asyncio
import time
import pytest
import pytest_asyncio

from nostr_sdk import Keys

from satorilib.satori_nostr import SatoriNostr, SatoriNostrConfig
from satorilib.satori_nostr.models import PredictionSubmission


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
            )


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
            )
            await asyncio.sleep(0.3)
            # Eavesdropper queue should be empty — can't decrypt
            assert eavesdropper._prediction_queue.empty()
        finally:
            await predictor.stop()
            await eavesdropper.stop()
