"""Central auth signs only EIP-191 (Ethereum personal_sign).

Every signature satorilib sends to central for authentication (POST
/api/v1/auth/login, and the legacy header auth that _makeAuthenticatedCall
falls back to via authPayload) is

    "0x" + r || s || v  (65 bytes as hex, v in {27, 28}, low s)

over keccak256("\\x19Ethereum Signed Message:\\n" + len(utf8 message) + message),
made by the signing key itself (the alias). Central recovers the signer and
compares it with the Ethereum address of the `alias-pubkey` header (the
compressed pubkey). There is no Evrmore signature on these paths.

The fixed vector is the one central's tests use (a throwaway key,
sha256("satori central eip191 login test vector")), so both sides are pinned to
the same bytes. It was checked independently with viem.
"""

import base64
import hashlib
import json
import re
from unittest.mock import MagicMock, patch

import pytest
import yaml
from eth_account import Account
from eth_account.messages import encode_defunct
from eth_keys import keys

from satorilib.server.server import SatoriServerClient
from satorilib.wallet.evrmore.identity import EvrmoreIdentity
from satorilib.wallet.evrmore.wallet import EvrmoreWallet

# The shared throwaway vector (same as central's tests/test_auth_login_signatures.py).
PRIVATE_KEY = hashlib.sha256(b"satori central eip191 login test vector").digest()
PUBKEY = "03a3a5a386e0b6af81e54dac571961f68a6d37183f41f39a297feea28ae8add34a"
ADDRESS = "0x2F66D2d1DfA3cbF191dd26394d3b241C90B57E74"
EVRMORE_ADDRESS = "ERNue8bxEjwzQzEgWzcrD5CiXYk9NnKumL"
MESSAGE = "satori central eip191 login test 2026-10-16"
EIP191_SIG = (
    "0xd76997c6e1d644a32f7831b26092a8bd7b94ce654f82580ee16bf6378523ed4d"
    "552aec5cd3694719f7e2dcb8c7fe7076eabe3a787ae1c15cdce69f2c6ab97011"
    "1b"
)

SECP256K1_N = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
CENTRAL = "http://central.test"


def _wallet(identity: EvrmoreIdentity) -> EvrmoreWallet:
    return EvrmoreWallet(identity=identity, skipSave=True)


def _recover(message: str, signature: str) -> str:
    return Account.recover_message(encode_defunct(text=message), signature=signature)


def _ethAddressOfPubkey(pubkey: str) -> str:
    """What central compares the recovered signer with."""
    return keys.PublicKey.from_compressed_bytes(bytes.fromhex(pubkey)).to_checksum_address()


def _assertEip191Form(signature: str):
    assert isinstance(signature, str)
    assert re.fullmatch(r"0x[0-9a-f]{130}", signature), signature
    raw = bytes.fromhex(signature[2:])
    assert raw[64] in (27, 28)
    s = int.from_bytes(raw[32:64], "big")
    assert 0 < s <= SECP256K1_N // 2, "central accepts only the low-s form"


@pytest.fixture
def vectorWallet(tmp_path) -> EvrmoreWallet:
    path = tmp_path / "wallet.yaml"
    path.write_text(yaml.dump({"entropy": base64.b64encode(PRIVATE_KEY).decode()}))
    return _wallet(EvrmoreIdentity(walletPath=str(path)))


@pytest.fixture
def alias(tmp_path) -> EvrmoreWallet:
    """A fresh alias: unencrypted, as Lite keeps alias.yaml."""
    return _wallet(EvrmoreIdentity(walletPath=str(tmp_path / "alias.yaml")))


@pytest.fixture(scope="module")
def walletPath(tmp_path_factory) -> str:
    """A wallet file encrypted at rest with a password (slow to make, so one per module)."""
    path = str(tmp_path_factory.mktemp("wallet") / "wallet.yaml")
    EvrmoreIdentity(walletPath=path, password="wallet pass")  # creates it, encrypted
    return path


@pytest.fixture(scope="module")
def wallet(walletPath) -> EvrmoreWallet:
    """The wallet, unlocked with its password."""
    return _wallet(EvrmoreIdentity(walletPath=walletPath, password="wallet pass"))


@pytest.fixture
def noEvrmoreSigning():
    """Fail loudly if anything on an auth path signs the Evrmore way."""
    def refuse(*args, **kwargs):
        raise AssertionError("Evrmore signing used on a central auth path")
    with patch("satorilib.wallet.evrmore.identity.signMessage", refuse), \
            patch("satorilib.wallet.evrmore.wallet.signMessage", refuse):
        yield


def _loginResponse():
    response = MagicMock(status_code=200, ok=True)
    response.json.return_value = {
        "access_token": "access", "refresh_token": "refresh", "expires_in": 3600}
    return response


# The fixed vector ##########################################################


def test_vector_key_is_the_key(vectorWallet):
    assert vectorWallet.pubkey == PUBKEY
    assert vectorWallet.address == EVRMORE_ADDRESS
    assert vectorWallet.ethAddress == ADDRESS
    assert _ethAddressOfPubkey(vectorWallet.pubkey) == ADDRESS


def test_vector_signature(vectorWallet):
    signature = vectorWallet.ethSign(MESSAGE)
    assert signature == EIP191_SIG
    _assertEip191Form(signature)
    assert _recover(MESSAGE, signature) == ADDRESS


# Recovery is the key's ethAddress, for the alias and for the wallet #######


@pytest.mark.parametrize("which", ["alias", "wallet"])
def test_signature_recovers_to_ethAddress(which, request):
    key = request.getfixturevalue(which)
    message = "1760630400.123456"
    signature = key.ethSign(message)
    _assertEip191Form(signature)
    assert _recover(message, signature) == key.ethAddress
    assert _ethAddressOfPubkey(key.pubkey) == key.ethAddress


def test_alias_and_wallet_sign_with_their_own_keys(alias, wallet):
    assert alias.ethAddress != wallet.ethAddress
    assert _recover("m", wallet.ethSign("m")) == wallet.ethAddress


def test_locked_wallet_cannot_sign(walletPath):
    locked = EvrmoreIdentity(walletPath=walletPath)  # no password
    assert locked.isEncrypted
    with pytest.raises(Exception, match="locked"):
        locked.ethSign("m")


# authPayload: the legacy header auth #######################################


def test_authPayload_shape(vectorWallet, noEvrmoreSigning):
    payload = vectorWallet.authPayload(asDict=True, challenge=MESSAGE)
    # no Evrmore address: central reads only these three headers
    assert payload == {
        "message": MESSAGE,
        "alias-pubkey": PUBKEY,
        "signature": EIP191_SIG,
    }
    assert json.loads(vectorWallet.authPayload(challenge=MESSAGE)) == payload


def test_authPayload_without_challenge_signs_a_fresh_message(alias, noEvrmoreSigning):
    payload = alias.authPayload(asDict=True)
    assert payload["message"]
    _assertEip191Form(payload["signature"])
    assert _recover(payload["message"], payload["signature"]) == alias.ethAddress


# SatoriServerClient #########################################################


def test_login_signs_eip191(alias, noEvrmoreSigning):
    client = SatoriServerClient(alias=alias, url=CENTRAL)
    with patch("satorilib.server.server.requests.post", return_value=_loginResponse()) as post:
        client._login_with_jwt()
    (url,), kwargs = post.call_args
    headers = kwargs["headers"]
    assert url == CENTRAL + "/api/v1/auth/login"
    assert set(headers) == {"alias-pubkey", "message", "signature"}
    assert headers["alias-pubkey"] == alias.pubkey
    _assertEip191Form(headers["signature"])
    assert _recover(headers["message"], headers["signature"]) == alias.ethAddress
    assert client._access_token == "access"


@pytest.mark.parametrize("useWallet", [False, True])
def test_legacy_fallback_signs_eip191(alias, wallet, useWallet, noEvrmoreSigning):
    client = SatoriServerClient(alias=alias, url=CENTRAL)
    signer = wallet if useWallet else alias
    sent = {}

    def call(url, headers=None, data=None, timeout=None):
        sent.update(headers)
        return MagicMock(status_code=200, ok=True, text="ok")

    with patch.object(client, "_ensure_authenticated", side_effect=RuntimeError("no jwt")):
        client._makeAuthenticatedCall(
            function=call,
            endpoint="/api/v1/balance/get",
            challenge="challenge-from-central",
            signer=wallet if useWallet else None)
    assert sent["message"] == "challenge-from-central"
    assert sent["alias-pubkey"] == signer.pubkey
    _assertEip191Form(sent["signature"])
    assert _recover("challenge-from-central", sent["signature"]) == signer.ethAddress
