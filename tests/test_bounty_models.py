"""Tests for bounty announcement models (Phase 1).

Covers: KIND_BOUNTY_ANNOUNCE constant, BountyAnnouncement dataclass.
"""
import json
import pytest

from satorilib.satori_nostr.models import (
    KIND_BOUNTY_ANNOUNCE,
    BountyAnnouncement,
)


class TestKindConstants:

    def test_kind_bounty_announce_value(self):
        assert KIND_BOUNTY_ANNOUNCE == 34607


class TestBountyAnnouncement:

    @pytest.fixture
    def sample(self):
        return BountyAnnouncement(
            stream_name='btc-price-usd',
            stream_provider_pubkey='aabbcc',
            host_pubkey='ddeeff',
            pay_per_obs_sats=300,
            paid_predictors=3,
            competing_predictors=5,
            scoring_metric='mae',
            scoring_params={},
            horizon=1,
            active=True,
            timestamp=1711234567,
        )

    def test_fields(self, sample):
        assert sample.stream_name == 'btc-price-usd'
        assert sample.stream_provider_pubkey == 'aabbcc'
        assert sample.host_pubkey == 'ddeeff'
        assert sample.pay_per_obs_sats == 300
        assert sample.paid_predictors == 3
        assert sample.competing_predictors == 5
        assert sample.scoring_metric == 'mae'
        assert sample.scoring_params == {}
        assert sample.horizon == 1
        assert sample.active is True
        assert sample.timestamp == 1711234567

    def test_to_dict(self, sample):
        d = sample.to_dict()
        assert d['stream_name'] == 'btc-price-usd'
        assert d['pay_per_obs_sats'] == 300
        assert d['active'] is True

    def test_from_dict_roundtrip(self, sample):
        assert BountyAnnouncement.from_dict(sample.to_dict()) == sample

    def test_to_json_roundtrip(self, sample):
        assert BountyAnnouncement.from_json(sample.to_json()) == sample

    def test_d_tag(self, sample):
        """d tag must uniquely identify stream+host for parameterized replaceable events."""
        assert sample.d_tag() == 'btc-price-usd:aabbcc:ddeeff'

    def test_close_returns_inactive_copy(self, sample):
        closed = sample.close()
        assert closed.active is False
        assert closed.stream_name == sample.stream_name
        assert closed.host_pubkey == sample.host_pubkey

    def test_scoring_params_defaults_to_empty_dict(self):
        c = BountyAnnouncement(
            stream_name='x',
            stream_provider_pubkey='aa',
            host_pubkey='bb',
            pay_per_obs_sats=100,
            paid_predictors=1,
            competing_predictors=1,
            scoring_metric='mae',
            horizon=1,
            active=True,
            timestamp=0,
        )
        assert c.scoring_params == {}
