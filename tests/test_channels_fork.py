"""The paid-stream channel on a local anvil fork of Base, against the deployed
PaymentChannels v3 and SATORI, with throwaway keys: the buyer's wallet
opens a channel (one openWithPermit), its alias signs vouchers, the
seller's alias settles them with settleMany into the seller's wallet, the
buyer's alias asks to close and, after the 7-day grace period, withdraws
the rest back to the wallet.

Never a public chain: runs only when SATORI_FORK_RPC names a local fork, e.g.
    anvil --fork-url https://mainnet.base.org --port 8559
    SATORI_FORK_RPC=http://127.0.0.1:8559 pytest tests/test_channels_fork.py
SATORI is minted on the fork by the token's airdrop contract, impersonated: no one's SATORI moves.
Everything is undone at the end (evm_snapshot / evm_revert), clock included.
"""
import os
from urllib.parse import urlparse

import pytest

RPC = os.getenv('SATORI_FORK_RPC')
pytestmark = pytest.mark.skipif(
    not RPC or urlparse(RPC).hostname not in ('127.0.0.1', 'localhost'),
    reason='needs a local fork: SATORI_FORK_RPC=http://127.0.0.1:<port>')

CHAIN = 8453
TOKEN = '0xd0a5bFF2789230b0fb1Bd989fc7CE7084c947E17'  # SATORI on Base (Lite's base_config)
SATORI = 10 ** 18


@pytest.fixture
def fork():
    from web3 import Web3
    from satorilib.chain.evm import EvmClient
    client = EvmClient(RPC)
    w3 = client.w3
    assert w3.eth.chain_id == CHAIN, 'not a fork of Base'
    snap = w3.provider.make_request('evm_snapshot', [])['result']
    try:
        yield client
    finally:
        w3.provider.make_request('evm_revert', [snap])


def _fund(client, address, eth=10 ** 17, satori=0):
    w3 = client.w3
    w3.provider.make_request('anvil_setBalance', [address, hex(eth)])
    if not satori:
        return
    abi = [{'name': 'airdrop', 'type': 'function', 'stateMutability': 'view', 'inputs': [],
            'outputs': [{'name': '', 'type': 'address'}]},
           {'name': 'airdropMint', 'type': 'function', 'stateMutability': 'nonpayable',
            'inputs': [{'name': 'to', 'type': 'address'}, {'name': 'amount', 'type': 'uint256'}], 'outputs': []}]
    token = w3.eth.contract(address=TOKEN, abi=abi)
    minter = token.functions.airdrop().call()  # SatoriAirdrop: the token's only airdropMint caller
    w3.provider.make_request('anvil_impersonateAccount', [minter])
    w3.provider.make_request('anvil_setBalance', [minter, hex(10 ** 18)])
    h = token.functions.airdropMint(address, satori).transact({'from': minter, 'gas': 1_000_000})
    assert w3.eth.wait_for_transaction_receipt(h).status == 1
    w3.provider.make_request('anvil_stopImpersonatingAccount', [minter])
    assert _balance(client, address) == satori


def _balance(client, address):
    abi = [{'name': 'balanceOf', 'type': 'function', 'stateMutability': 'view',
            'inputs': [{'name': 'a', 'type': 'address'}], 'outputs': [{'name': '', 'type': 'uint256'}]}]
    return client.w3.eth.contract(address=TOKEN, abi=abi).functions.balanceOf(address).call()


def test_open_voucher_settle_close_withdraw_on_the_fork(fork):
    from eth_account import Account
    from satorilib.chain.channels import (
        CLOSE_GRACE_S, ChannelError, PaymentChannels, sign_binding, binding_signer, sign_voucher, verify_voucher)

    keys = {k: Account.create() for k in ('buyer_wallet', 'buyer_id', 'seller_wallet', 'seller_id')}
    for k, a in keys.items():
        _fund(fork, a.address, satori=5 * SATORI if k == 'buyer_wallet' else 0)
    chans = PaymentChannels(fork, CHAIN, TOKEN)
    price = 10 ** 15  # 0.001 SATORI
    deposit = 100 * price

    # the buyer's wallet opens the channel: payer = wallet, signer = alias, payee = the seller's wallet
    r = chans.open_channel(keys['buyer_wallet'].key.to_0x_hex(), keys['buyer_id'].address,
                           keys['seller_wallet'].address, deposit)
    assert r['permit'] is True  # one transaction, with SATORI's permit
    ch = chans.read_channel(r['id'])
    assert (ch['payer'], ch['payee'], ch['signer'], ch['deposit'], ch['settled']) == (
        keys['buyer_wallet'].address, keys['seller_wallet'].address, keys['buyer_id'].address, deposit, 0)
    assert ch['token'].lower() == TOKEN.lower() and not ch['closed'] and ch['close_requested_at'] is None
    assert (ch['grace'], ch['payout']) == (CLOSE_GRACE_S, keys['seller_wallet'].address)  # v3's getTerms, unchanged
    binding = sign_binding(keys['buyer_id'].key.to_0x_hex(), r['id'], '6' * 64)
    assert binding_signer(r['id'], '6' * 64, binding) == ch['signer']

    # the alias signs a voucher per observation; the seller checks each against the channel's signer
    vouchers = [sign_voucher(keys['buyer_id'].key.to_0x_hex(), CHAIN, r['id'], n * price) for n in range(1, 4)]
    assert all(verify_voucher(ch['signer'], v, CHAIN, r['id'], (n + 1) * price) for n, v in enumerate(vouchers))

    # the seller's alias settles the newest with settleMany, into the seller's wallet
    s = chans.settle_many(keys['seller_id'].key.to_0x_hex(), [{'id': r['id'], 'cumulative': 3 * price,
                                                                'signature': vouchers[-1]}])
    assert s['skipped'] == [] and s['settled'] == [{'id': r['id'], 'cumulative': 3 * price, 'paid': 3 * price,
                                                    'settled': 3 * price}]
    assert _balance(fork, keys['seller_wallet'].address) == 3 * price
    again = chans.settle_many(keys['seller_id'].key.to_0x_hex(), [{'id': r['id'], 'cumulative': 3 * price,
                                                                    'signature': vouchers[-1]}])
    assert again == {'hash': None, 'settled': [], 'skipped': [{'id': r['id'], 'reason': 'nothing-due'}]}

    # the buyer's alias asks to close; withdraw is refused in plain words before the grace period
    chans.request_close(keys['buyer_id'].key.to_0x_hex(), r['id'])
    ch = chans.read_channel(r['id'])
    assert ch['close_requested_at'] and ch['grace_ends_at'] == ch['close_requested_at'] + CLOSE_GRACE_S
    with pytest.raises(ChannelError, match="The waiting period after the close request hasn't ended yet."):
        chans.withdraw(keys['buyer_id'].key.to_0x_hex(), r['id'])

    # 8 days on: the alias withdraws, the rest goes back to the wallet (the payer)
    fork.w3.provider.make_request('evm_increaseTime', [8 * 86400])
    fork.w3.provider.make_request('evm_mine', [])
    chans.withdraw(keys['buyer_id'].key.to_0x_hex(), r['id'])
    assert chans.read_channel(r['id'])['closed'] is True
    assert _balance(fork, keys['buyer_wallet'].address) == 5 * SATORI - 3 * price


HUB = '0x195083eE270ad89459e8dbF2d40fDFc85fb21327'  # SatoriHub on Base (Lite's base_config)


def test_a_staked_wallet_cant_fund_a_channel_said_in_plain_words(fork):
    """A wallet staked with its neuron (delegatePredictionPower to the alias): SATORI's transfer lock."""
    from eth_account import Account
    from satorilib.chain.channels import HOOK_GAS, ChannelError, PaymentChannels

    wallet, alias = Account.create(), Account.create()
    _fund(fork, wallet.address, satori=SATORI)
    hub = fork.contract(HUB, [{'name': 'delegatePredictionPower', 'type': 'function', 'stateMutability': 'nonpayable',
                               'inputs': [{'name': 'delegate', 'type': 'address'}], 'outputs': []}])
    fork.send(hub.functions.delegatePredictionPower(alias.address), private_key=wallet.key.to_0x_hex(),
              gas_pad=HOOK_GAS)
    chans = PaymentChannels(fork, CHAIN, TOKEN)
    assert chans._lock_state(wallet.address)[0] == 2 ** 24 - 1
    before = fork.w3.eth.get_transaction_count(wallet.address)
    with pytest.raises(ChannelError, match="Your wallet is staked with this neuron, so it can't send SATORI: "
                                           "a channel needs SATORI the wallet can send."):
        chans.open_channel(wallet.key.to_0x_hex(), alias.address, Account.create().address, 10 ** 15)
    assert fork.w3.eth.get_transaction_count(wallet.address) == before  # nothing sent
