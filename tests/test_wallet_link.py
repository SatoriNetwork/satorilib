"""The wallet's link to its alias, and register in the current form.

The link is an EIP-191 (personal_sign) signature by the wallet of exactly

    Satori: link this alias to my wallet
    alias: 0x<the alias's address, EIP-55>
    wallet: 0x<the wallet's address, EIP-55>
    issued: <unix seconds>

Register sends alias-pubkey, nostr-pubkey, neuron-version, and the link as
wallet-link (base64 of the message's UTF-8) and wallet-signature. Its answer
names wallet_address (the linked wallet's Base address, or null) and may carry
wallet_link_error.
"""

import base64
import hashlib
from unittest.mock import MagicMock, patch

import pytest
import yaml
from eth_account import Account
from eth_account.messages import encode_defunct

from satorilib.server.server import SatoriServerClient
from satorilib.wallet.evrmore.identity import EvrmoreIdentity
from satorilib.wallet.evrmore.wallet import EvrmoreWallet
from satorilib.wallet.link import (
    LINK_TITLE, linkFields, linkHeaders, linkIsFor, linkMessage, makeLink)

CENTRAL = "http://central.test"
NOSTR = "ab" * 32


def _key(tmp_path, name: str, seed: bytes) -> EvrmoreWallet:
    path = tmp_path / f"{name}.yaml"
    path.write_text(yaml.dump({"entropy": base64.b64encode(hashlib.sha256(seed).digest()).decode()}))
    return EvrmoreWallet(identity=EvrmoreIdentity(walletPath=str(path)), skipSave=True)


@pytest.fixture
def alias(tmp_path) -> EvrmoreWallet:
    return _key(tmp_path, "alias", b"satori link test alias")


@pytest.fixture
def wallet(tmp_path) -> EvrmoreWallet:
    return _key(tmp_path, "wallet", b"satori link test wallet")


def test_message_is_exactly_four_lines(alias, wallet):
    message = linkMessage(alias.ethAddress.lower(), wallet.ethAddress.lower(), 1761200000)
    assert message == (
        "Satori: link this alias to my wallet\n"
        f"alias: {alias.ethAddress}\n"
        f"wallet: {wallet.ethAddress}\n"
        "issued: 1761200000")
    assert message.split("\n")[0] == LINK_TITLE
    assert alias.ethAddress != alias.ethAddress.lower()  # EIP-55, not lower case


def test_link_is_signed_by_the_wallet(alias, wallet):
    link = makeLink(wallet, alias.ethAddress, issued=1761200000)
    assert set(link) == {"message", "signature"}
    assert linkFields(link["message"]) == {
        "alias": alias.ethAddress, "wallet": wallet.ethAddress, "issued": 1761200000}
    signer = Account.recover_message(encode_defunct(text=link["message"]), signature=link["signature"])
    assert signer == wallet.ethAddress


def test_issued_defaults_to_now(alias, wallet):
    with patch("satorilib.wallet.link.time.time", return_value=1761200123.9):
        link = makeLink(wallet, alias.ethAddress)
    assert linkFields(link["message"])["issued"] == 1761200123


def test_link_is_for_this_alias_and_wallet_only(tmp_path, alias, wallet):
    link = makeLink(wallet, alias.ethAddress)
    other = _key(tmp_path, "other", b"satori link test other")
    assert linkIsFor(link, alias.ethAddress, wallet.ethAddress)
    assert not linkIsFor(link, other.ethAddress, wallet.ethAddress)
    assert not linkIsFor(link, alias.ethAddress, other.ethAddress)
    assert not linkIsFor(None, alias.ethAddress, wallet.ethAddress)
    forged = {"message": link["message"], "signature": other.ethSign(link["message"])}
    assert not linkIsFor(forged, alias.ethAddress, wallet.ethAddress)


def test_headers_carry_the_message_as_base64_of_its_utf8(alias, wallet):
    link = makeLink(wallet, alias.ethAddress)
    headers = linkHeaders(link)
    assert set(headers) == {"wallet-link", "wallet-signature"}
    assert "\n" not in headers["wallet-link"]
    assert base64.b64decode(headers["wallet-link"]).decode("utf-8") == link["message"]
    assert headers["wallet-signature"] == link["signature"]
    assert linkHeaders(None) == {}


# register #################################################################


def _answer(body: dict):
    response = MagicMock(status_code=200, ok=True, text=str(body))
    response.json.return_value = body
    return response


def test_register_without_a_link_sends_the_alias_alone(alias):
    client = SatoriServerClient(alias=alias, url=CENTRAL)
    with patch("satorilib.server.server.requests.post",
               return_value=_answer({"success": True, "peer_id": 7, "wallet_address": None})) as post:
        answer = client.register(nostrPubkey=NOSTR, version="1.2.3")
    (url,), kwargs = post.call_args
    assert url == CENTRAL + "/api/v1/peer/register"
    assert kwargs["headers"] == {
        "alias-pubkey": alias.pubkey, "nostr-pubkey": NOSTR, "neuron-version": "1.2.3"}
    assert answer["wallet_address"] is None


def test_register_sends_the_link(alias, wallet):
    link = makeLink(wallet, alias.ethAddress)
    client = SatoriServerClient(alias=alias, url=CENTRAL)
    with patch("satorilib.server.server.requests.post",
               return_value=_answer({"success": True, "wallet_address": wallet.ethAddress})) as post:
        client.register(walletLink=link, nostrPubkey=NOSTR, version="1.2.3")
    headers = post.call_args.kwargs["headers"]
    assert set(headers) == {
        "alias-pubkey", "nostr-pubkey", "neuron-version", "wallet-link", "wallet-signature"}
    assert "wallet-pubkey" not in headers and "vault-pubkey" not in headers
    assert base64.b64decode(headers["wallet-link"]).decode() == link["message"]


def test_register_refusal_raises(alias):
    client = SatoriServerClient(alias=alias, url=CENTRAL)
    refused = MagicMock(status_code=409, ok=False, text="taken")
    with patch("satorilib.server.server.requests.post", return_value=refused):
        with pytest.raises(Exception, match="409"):
            client.register()


def test_checkin_reads_the_wallet_address_and_link_error(alias, wallet):
    link = makeLink(wallet, alias.ethAddress)
    client = SatoriServerClient(alias=alias, url=CENTRAL)
    body = {"success": True, "wallet_address": None, "wallet_link_error": "issued is too old"}
    with patch("satorilib.server.server.requests.post", return_value=_answer(body)), \
            patch.object(client, "_getChallenge", return_value="c"):
        result = client.checkin(walletLink=link, nostrPubkey=NOSTR, version="1")
    assert result["walletAddress"] is None
    assert result["walletLinkError"] == "issued is too old"

    body = {"success": True, "wallet_address": wallet.ethAddress}
    with patch("satorilib.server.server.requests.post", return_value=_answer(body)), \
            patch.object(client, "_getChallenge", return_value="c"):
        result = client.checkin(walletLink=link)
    assert result["walletAddress"] == wallet.ethAddress
    assert result["walletLinkError"] is None


def test_checkin_registers_before_a_wallet_exists(alias):
    client = SatoriServerClient(alias=alias, url=CENTRAL)
    with patch("satorilib.server.server.requests.post",
               return_value=_answer({"success": True, "wallet_address": None})) as post, \
            patch.object(client, "_getChallenge", return_value="c"):
        client.checkin(nostrPubkey=NOSTR)
    assert post.call_count == 1
    assert post.call_args.kwargs["headers"]["alias-pubkey"] == alias.pubkey
