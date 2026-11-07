"""Payment channels on Base (satorilib.chain.channels), checked against the
browser neuron's fixtures so both sides make and accept the same bytes:
channel ids and voucher digests the deployed contract computed
(fixtures/channels-vectors.json), and voucher and binding signatures the
browser's own code made (fixtures/channels-signatures.json)."""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from satorilib.chain import channels as pc
from satorilib.chain.channels import (
    ADDRESS, ChannelError, PaymentChannels, binding_signer, binding_text, channel_error_text,
    channel_id_of, due_of, lock_error, recover_voucher_signer, sign_binding, sign_voucher,
    transfer_lock, verify_voucher, voucher_digest,
)

FIX = Path(__file__).parent / 'fixtures'
VEC = json.loads((FIX / 'channels-vectors.json').read_text())
SIG = json.loads((FIX / 'channels-signatures.json').read_text())
CHAIN = VEC['chainId']
KEY = SIG['key']
OTHER = '0x' + '33' * 32
CH = SIG['channelId']


def test_the_address_is_the_deployed_contract():
    assert ADDRESS == VEC['contract']


@pytest.mark.parametrize('v', VEC['ids'])
def test_channel_id_is_the_contracts_compute_channel_id(v):
    assert channel_id_of(v['payer'], v['payee'], v['token'], v['signer'], v['salt']) == v['id']


@pytest.mark.parametrize('v', VEC['digests'])
def test_voucher_digest_is_the_contracts(v):
    assert voucher_digest(CHAIN, v['channelId'], int(v['cumulative'])) == v['digest']


@pytest.mark.parametrize('v', SIG['vouchers'])
def test_a_voucher_signs_to_the_browsers_bytes_and_recovers(v):
    c = int(v['cumulative'])
    assert voucher_digest(CHAIN, CH, c) == v['digest']
    sig = sign_voucher(KEY, CHAIN, CH, c)
    assert sig == v['signature']
    assert recover_voucher_signer(sig, CHAIN, CH, c) == SIG['address']
    assert verify_voucher(SIG['address'], sig, CHAIN, CH, c)


def test_verify_voucher_refuses_another_signer_channel_amount_chain_or_contract():
    sig = sign_voucher(KEY, CHAIN, CH, 1000)
    me = SIG['address']
    assert verify_voucher(me, sig, CHAIN, CH, 1000)
    assert not verify_voucher('0x' + '1' * 40, sig, CHAIN, CH, 1000)
    assert not verify_voucher(me, sig, CHAIN, VEC['ids'][0]['id'], 1000)
    assert not verify_voucher(me, sig, CHAIN, CH, 1001)
    assert not verify_voucher(me, sig, 8453, CH, 1000)
    assert not verify_voucher(me, sig, CHAIN, CH, 1000, contract='0x' + '2' * 40)
    assert not verify_voucher(me, sign_voucher(OTHER, CHAIN, CH, 1000), CHAIN, CH, 1000)


def test_signatures_the_contracts_ecdsa_refuses_are_refused():
    sig = sign_voucher(KEY, CHAIN, CH, 1000)
    r, s, v = int(sig[2:66], 16), int(sig[66:130], 16), int(sig[130:], 16)
    n = 0xfffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141
    twin = '0x' + f'{r:064x}{n - s:064x}{(55 - v):02x}'  # the malleable twin: high s, flipped v
    assert recover_voucher_signer(twin, CHAIN, CH, 1000) is None
    assert recover_voucher_signer(sig[:130] + f'{v - 27:02x}', CHAIN, CH, 1000) is None  # v 0/1
    assert recover_voucher_signer(sig[:130], CHAIN, CH, 1000) is None  # 64 bytes
    assert recover_voucher_signer('0x1234', CHAIN, CH, 1000) is None
    assert recover_voucher_signer(None, CHAIN, CH, 1000) is None
    assert recover_voucher_signer(sig, CHAIN, CH, 2 ** 96) is None  # past uint96


def test_due_of():
    assert due_of(100, 0, 30) == 30
    assert due_of(100, 30, 30) == 0
    assert due_of(100, 30, 250) == 70  # never past the deposit
    assert due_of(100, 50, 20) == 0


def test_the_binding_is_the_identitys_eip191_signature_of_the_channel_and_nostr_key():
    b = SIG['binding']
    assert binding_text(CH.upper().replace('0X', '0x'), b['nostr']) == b['text']  # the id lowercased
    assert sign_binding(KEY, CH, b['nostr']) == b['signature']
    assert binding_signer(CH, b['nostr'], b['signature']) == SIG['address']
    assert binding_signer(CH, '7' * 64, b['signature']) != SIG['address']
    assert binding_signer(VEC['ids'][0]['id'], b['nostr'], b['signature']) != SIG['address']
    assert binding_signer(CH, b['nostr'], '0x1234') is None
    assert binding_signer(CH, b['nostr'], None) is None


def test_the_transfer_lock_in_plain_words():
    assert transfer_lock(0, 20747) == {'locked': False, 'indefinite': False, 'until_round': None, 'free_at': None}
    assert lock_error(transfer_lock(20746, 20747)) is None  # ended
    staked = lock_error(transfer_lock(2 ** 24 - 1, 20747))
    assert str(staked) == ("Your wallet is staked with this neuron, so it can't send SATORI: "
                           "a channel needs SATORI the wallet can send.")
    assert staked.reason == 'Locked'
    after = lock_error(transfer_lock(20747, 20747))
    assert str(after) == ("Your wallet's SATORI can't be sent until Oct 22, 00:00 UTC (round 20748): "
                          "a channel needs SATORI the wallet can send.")


def test_contract_errors_in_plain_words():
    from eth_utils import keccak

    class Custom(Exception):
        def __init__(self, data):
            super().__init__(data)
            self.data = data

    def err(sig, args=''):
        return Custom('0x' + keccak(text=sig)[:4].hex() + args)

    assert channel_error_text(err('CloseNotReady()')) == "The 7-day grace period after the close request hasn't ended yet."
    assert channel_error_text(err('Locked()')) == ("The wallet's SATORI can't be sent now: it is staked, "
                                                   "or it predicted by hand this round or the last.")
    assert channel_error_text(err('ERC20InsufficientBalance(address,uint256,uint256)', '00' * 96)) == 'Not enough SATORI in the wallet.'
    assert channel_error_text(err('HookFailedWithoutReason()')) == 'The transaction ran out of gas; try again.'
    assert channel_error_text(ChannelError('as it is')) == 'as it is'
    assert channel_error_text(RuntimeError('something else')) == 'something else'


# ── the client, over a stubbed chain ─────────────────────────────────────────

ZERO = '0x' + '0' * 40
SIGNER = SIG['address']


class _Fn:
    def __init__(self, result):
        self.result = result

    def call(self):
        return self.result


class _Contract:
    """getChannel answered from `held`; every write recorded with its gas pad."""

    def __init__(self, held):
        self.held = held
        self.functions = SimpleNamespace(
            getChannel=lambda cid: _Fn(self.held.get('0x' + bytes(cid).hex(), (ZERO, 0, ZERO, 0, ZERO, 0, False, ZERO))),
            settleMany=lambda ids, cums, sigs: ('settleMany', ['0x' + bytes(i).hex() for i in ids], list(cums)),
            requestClose=lambda cid: ('requestClose', '0x' + bytes(cid).hex()),
            withdraw=lambda cid: ('withdraw', '0x' + bytes(cid).hex()))
        self.events = SimpleNamespace(Settled=lambda: SimpleNamespace(process_receipt=lambda r, errors=None: []))


class _Client:
    def __init__(self, contract):
        self._contract = contract
        self.sent = []
        self.w3 = SimpleNamespace(eth=SimpleNamespace(get_transaction_receipt=lambda h: {'logs': []}))

    def contract(self, address, abi):
        return self._contract

    def send(self, fn, private_key=None, gas_pad=0):
        self.sent.append((fn, gas_pad))
        return '0xhash'


def _channel(deposit=100, settled=0, closed=False, payer=SIGNER):
    return (payer, deposit, '0x' + '2' * 40, settled, '0x' + '4' * 40, 0, closed, SIGNER)


def test_settle_many_sends_only_what_would_pay_padded_per_payment():
    ids = ['0x' + c * 64 for c in '123']
    held = {ids[0]: _channel(), ids[1]: _channel(closed=True), ids[2]: _channel(settled=40), CH: _channel()}
    client = _Client(_Contract(held))
    chans = PaymentChannels(client, CHAIN, token='0x' + '4' * 40)
    entries = [
        {'id': ids[0], 'cumulative': 30, 'signature': sign_voucher(KEY, CHAIN, ids[0], 30)},
        {'id': ids[1], 'cumulative': 30, 'signature': sign_voucher(KEY, CHAIN, ids[1], 30)},
        {'id': ids[2], 'cumulative': 40, 'signature': sign_voucher(KEY, CHAIN, ids[2], 40)},
        {'id': CH, 'cumulative': 30, 'signature': sign_voucher(OTHER, CHAIN, CH, 30)},
        {'id': '0x' + 'ab' * 32, 'cumulative': 30, 'signature': sign_voucher(KEY, CHAIN, '0x' + 'ab' * 32, 30)},
    ]
    r = chans.settle_many(OTHER, entries)
    assert r['skipped'] == [{'id': ids[1], 'reason': 'closed'}, {'id': ids[2], 'reason': 'nothing-due'},
                            {'id': CH, 'reason': 'bad-signature'}, {'id': '0x' + 'ab' * 32, 'reason': 'unknown'}]
    assert client.sent == [(('settleMany', [ids[0]], [30]), pc.HOOK_GAS + pc.PAYMENT_GAS)]
    assert r['hash'] == '0xhash'


def test_settle_many_with_nothing_to_pay_sends_nothing():
    client = _Client(_Contract({CH: _channel(settled=30)}))
    r = PaymentChannels(client, CHAIN, token='0x' + '4' * 40).settle_many(
        OTHER, [{'id': CH, 'cumulative': 30, 'signature': sign_voucher(KEY, CHAIN, CH, 30)}])
    assert r == {'hash': None, 'settled': [], 'skipped': [{'id': CH, 'reason': 'nothing-due'}]}
    assert client.sent == []


def test_close_and_withdraw_are_the_identitys_and_padded():
    client = _Client(_Contract({}))
    chans = PaymentChannels(client, CHAIN, token='0x' + '4' * 40)
    chans.request_close(KEY, CH)
    chans.withdraw(KEY, CH)
    assert client.sent == [(('requestClose', CH), pc.HOOK_GAS), (('withdraw', CH), pc.HOOK_GAS)]


def test_read_channel():
    client = _Client(_Contract({CH: (SIGNER, 100, '0x' + '2' * 40, 30, '0x' + '4' * 40, 1_792_100_000, False, SIGNER)}))
    chans = PaymentChannels(client, CHAIN, token='0x' + '4' * 40)
    assert chans.read_channel(CH) == {
        'id': CH, 'payer': SIGNER, 'payee': '0x' + '2' * 40, 'token': '0x' + '4' * 40, 'signer': SIGNER,
        'deposit': 100, 'settled': 30, 'close_requested_at': 1_792_100_000, 'closed': False,
        'grace_ends_at': 1_792_100_000 + pc.CLOSE_GRACE_S}
    client._contract.held[CH] = (ZERO, 0, ZERO, 0, ZERO, 0, False, ZERO)
    assert chans.read_channel(CH) is None


def test_a_staked_wallet_is_refused_before_anything_is_signed_or_sent():
    client = _Client(_Contract({}))
    chans = PaymentChannels(client, CHAIN, token='0x' + '4' * 40)
    chans._lock_state = lambda wallet: (2 ** 24 - 1, 20747)
    chans._permit = lambda *a: pytest.fail('signed a permit')
    with pytest.raises(ChannelError, match="Your wallet is staked with this neuron, so it can't send SATORI"):
        chans.open_channel(KEY, SIGNER, '0x' + '2' * 40, 100)
    assert client.sent == []
