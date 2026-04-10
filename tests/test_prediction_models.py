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
            seq_num=1042,
            predicted_value=67450.25,
            timestamp=1711234560,
        )

    def test_fields(self, sample):
        assert sample.stream_name == 'btc-price-usd'
        assert sample.stream_provider_pubkey == 'aabbcc'
        assert sample.predictor_pubkey == 'ddeeff'
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

    def test_stream_key(self, sample):
        """Unique key for grouping predictions by stream."""
        assert sample.stream_key() == 'btc-price-usd:aabbcc'
