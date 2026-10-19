"""setRewardAddress: central refusing the address is an answer, not an auth failure.

Central answers 400 "Invalid Base address" for an address it won't take. That
must come back as (False, body) from the first call, so the caller can show
central's reason. It must not fall into the legacy-auth retry, which repeats
the request with other headers and then raises, losing the reason.
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import requests

from satorilib.server.server import SatoriServerClient

CENTRAL = "http://central.test"
BODY = '{"detail":"Invalid Base address"}'


def _response(status: int, text: str) -> requests.Response:
    r = requests.Response()
    r.status_code = status
    r._content = text.encode()
    r.url = CENTRAL + "/api/v1/peer/reward-address"
    return r


def _client():
    client = SatoriServerClient(wallet=MagicMock(), url=CENTRAL)
    client._access_token = "access"
    client._token_expiry = datetime.now() + timedelta(hours=1)
    return client


def test_refused_address_returns_central_reason_without_legacy_retry():
    client = _client()
    refused = _response(400, BODY)
    with patch("satorilib.server.server.requests.post", return_value=refused) as post, \
            patch.object(client, "_getChallenge", side_effect=AssertionError("legacy retry")):
        assert client.setRewardAddress(address="ERNue8bxEjwzQzEgWzcrD5CiXYk9NnKumL") == (False, BODY)
    assert post.call_count == 1


def test_accepted_address():
    client = _client()
    ok = _response(200, '{"reward_address":"0x2F66D2d1DfA3cbF191dd26394d3b241C90B57E74"}')
    with patch("satorilib.server.server.requests.post", return_value=ok) as post:
        assert client.setRewardAddress(address="0x2F66D2d1DfA3cbF191dd26394d3b241C90B57E74")[0] is True
    (url,), kwargs = post.call_args
    assert url == CENTRAL + "/api/v1/peer/reward-address"
    assert kwargs["headers"]["Authorization"] == "Bearer access"
