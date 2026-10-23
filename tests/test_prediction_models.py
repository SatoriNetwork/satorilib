"""Tests for prediction submission models (Phase 2).

Covers: KIND_PREDICTION constant, PredictionSubmission dataclass.
"""
import json
import pytest

from satorilib.satori_nostr.models import (
    KIND_PREDICTION,
    PredictionSubmission,
)


class TestKindConstants:

    def test_kind_prediction_value(self):
        assert KIND_PREDICTION == 34608


class TestPredictionSubmission:

    @pytest.fixture
    def sample(self):
        return PredictionSubmission(
            stream_name='btc-price-usd',
            stream_provider_pubkey='aabbcc',
            predictor_pubkey='ddeeff',
            predictor_alias_pubkey='11aa22bb',
            seq_num=1042,
            predicted_value=67450.25,
            timestamp=1711234560,
        )

    def test_fields(self, sample):
        assert sample.stream_name == 'btc-price-usd'
        assert sample.stream_provider_pubkey == 'aabbcc'
        assert sample.predictor_pubkey == 'ddeeff'
        assert sample.predictor_alias_pubkey == '11aa22bb'
        assert sample.seq_num == 1042
        assert sample.predicted_value == 67450.25
        assert sample.timestamp == 1711234560

    def test_to_dict(self, sample):
        d = sample.to_dict()
        assert d['stream_name'] == 'btc-price-usd'
        assert d['predicted_value'] == 67450.25

    def test_from_dict_roundtrip(self, sample):
        assert PredictionSubmission.from_dict(sample.to_dict()) == sample

    def test_to_json_roundtrip(self, sample):
        assert PredictionSubmission.from_json(sample.to_json()) == sample

    def test_writes_predictor_alias_pubkey_only(self, sample):
        d = sample.to_dict()
        assert d['predictor_alias_pubkey'] == '11aa22bb'
        assert 'predictor_wallet_pubkey' not in d

    def test_reads_an_older_neurons_predictor_wallet_pubkey_as_the_alias(self, sample):
        older = sample.to_dict()
        older['predictor_wallet_pubkey'] = older.pop('predictor_alias_pubkey')
        assert PredictionSubmission.from_dict(older) == sample

    def test_stream_key(self, sample):
        """Unique key for grouping predictions by stream."""
        assert sample.stream_key() == 'btc-price-usd:aabbcc'
