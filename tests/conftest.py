"""Shared fixtures for satori_nostr tests.

Uses an in-process mini Nostr relay (mini_relay.py) so tests run without
any external relay or Docker dependency.
"""
import asyncio
import pytest
import pytest_asyncio
from nostr_sdk import Keys, SecretKey

from tests.mini_relay import MiniRelay


@pytest_asyncio.fixture
async def relay_url():
    """Start a fresh mini relay for each test, return its URL."""
    relay = MiniRelay()
    await relay.start()
    yield relay.url
    await relay.stop()


@pytest.fixture
def provider_keys():
    return Keys.generate()


@pytest.fixture
def subscriber_keys():
    return Keys.generate()
