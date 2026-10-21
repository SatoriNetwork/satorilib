"""The paid-stream channel on a local anvil fork of Base Sepolia, against the
deployed PaymentChannels and SATORI, with throwaway keys: the buyer's vault
opens a channel (one openWithPermit), its identity signs vouchers, the
seller's identity settles them with settleMany into the seller's vault, the
buyer's identity asks to close and, after the 7-day grace period, withdraws
the rest back to the vault.

Never a public chain: runs only when SATORI_FORK_RPC names a local fork, e.g.
    node Satori/satori/scripts/fork.mjs start --port 8559
    SATORI_FORK_RPC=http://127.0.0.1:8559 pytest tests/test_channels_fork.py
Everything is undone at the end (evm_snapshot / evm_revert), clock included.
"""
import os
from urllib.parse import urlparse

import pytest

RPC = os.getenv('SATORI_FORK_RPC')
pytestmark = pytest.mark.skipif(
    not RPC or urlparse(RPC).hostname not in ('127.0.0.1', 'localhost'),
    reason='needs a local fork: SATORI_FORK_RPC=http://127.0.0.1:<port>')

TOKEN = '0x4dee947383d5C9c8E421F55Fe3518a29324aE469'
DEPLOYER = '0x9d3109b8A045D63056Aa5C1d2c883F3F9fA18817'  # holds SATORI on Base Sepolia (Satori/satori scripts/fork.mjs)
SATORI = 10 ** 18


@pytest.fixture
def fork():
    from web3 import Web3
    from satorilib.chain.evm import EvmClient
    client = EvmClient(RPC)
    w3 = client.w3
    assert w3.eth.chain_id == 84532, 'not a fork of Base Sepolia'
    snap = w3.provider.make_request('evm_snapshot', [])['result']
    try:
        yield client
    finally:
        w3.provider.make_request('evm_revert', [snap])


def _fund(client, address, eth=10 ** 17, satori=0):
    from satorilib.chain.channels import HOOK_GAS, TOKEN_ABI
    w3 = client.w3
    w3.provider.make_request('anvil_setBalance', [address, hex(eth)])
    if not satori:
        return
    w3.provider.make_request('anvil_impersonateAccount', [DEPLOYER])
    w3.provider.make_request('anvil_setBalance', [DEPLOYER, hex(10 * SATORI)])
    abi = TOKEN_ABI + [{'name': 'transfer', 'type': 'function', 'stateMutability': 'nonpayable',
                        'inputs': [{'name': 'to', 'type': 'address'}, {'name': 'value', 'type': 'uint256'}],
                        'outputs': [{'name': '', 'type': 'bool'}]}]
    token = w3.eth.contract(address=TOKEN, abi=abi)
    fn = token.functions.transfer(address, satori)
    gas = fn.estimate_gas({'from': DEPLOYER}) + HOOK_GAS
    h = fn.transact({'from': DEPLOYER, 'gas': gas})
    assert w3.eth.wait_for_transaction_receipt(h).status == 1


def _balance(client, address):
    abi = [{'name': 'balanceOf', 'type': 'function', 'stateMutability': 'view',
            'inputs': [{'name': 'a', 'type': 'address'}], 'outputs': [{'name': '', 'type': 'uint256'}]}]
    return client.w3.eth.contract(address=TOKEN, abi=abi).functions.balanceOf(address).call()


def test_open_voucher_settle_close_withdraw_on_the_fork(fork):
    from eth_account import Account
    from satorilib.chain.channels import (
        CLOSE_GRACE_S, ChannelError, PaymentChannels, sign_binding, binding_signer, sign_voucher, verify_voucher)

    keys = {k: Account.create() for k in ('buyer_vault', 'buyer_id', 'seller_vault', 'seller_id')}
    for k, a in keys.items():
        _fund(fork, a.address, satori=5 * SATORI if k == 'buyer_vault' else 0)
    chans = PaymentChannels(fork, 84532, TOKEN)
    price = 10 ** 15  # 0.001 SATORI
    deposit = 100 * price

    # the buyer's vault opens the channel: payer = vault, signer = identity, payee = the seller's vault
    r = chans.open_channel(keys['buyer_vault'].key.to_0x_hex(), keys['buyer_id'].address,
                           keys['seller_vault'].address, deposit)
    assert r['permit'] is True  # one transaction, with SATORI's permit
    ch = chans.read_channel(r['id'])
    assert (ch['payer'], ch['payee'], ch['signer'], ch['deposit'], ch['settled']) == (
        keys['buyer_vault'].address, keys['seller_vault'].address, keys['buyer_id'].address, deposit, 0)
    assert ch['token'].lower() == TOKEN.lower() and not ch['closed'] and ch['close_requested_at'] is None
    binding = sign_binding(keys['buyer_id'].key.to_0x_hex(), r['id'], '6' * 64)
    assert binding_signer(r['id'], '6' * 64, binding) == ch['signer']

    # the identity signs a voucher per observation; the seller checks each against the channel's signer
    vouchers = [sign_voucher(keys['buyer_id'].key.to_0x_hex(), 84532, r['id'], n * price) for n in range(1, 4)]
    assert all(verify_voucher(ch['signer'], v, 84532, r['id'], (n + 1) * price) for n, v in enumerate(vouchers))

    # the seller's identity settles the newest with settleMany, into the seller's vault
    s = chans.settle_many(keys['seller_id'].key.to_0x_hex(), [{'id': r['id'], 'cumulative': 3 * price,
                                                                'signature': vouchers[-1]}])
    assert s['skipped'] == [] and s['settled'] == [{'id': r['id'], 'cumulative': 3 * price, 'paid': 3 * price,
                                                    'settled': 3 * price}]
    assert _balance(fork, keys['seller_vault'].address) == 3 * price
    again = chans.settle_many(keys['seller_id'].key.to_0x_hex(), [{'id': r['id'], 'cumulative': 3 * price,
                                                                    'signature': vouchers[-1]}])
    assert again == {'hash': None, 'settled': [], 'skipped': [{'id': r['id'], 'reason': 'nothing-due'}]}

    # the buyer's identity asks to close; withdraw is refused in plain words before the grace period
    chans.request_close(keys['buyer_id'].key.to_0x_hex(), r['id'])
    ch = chans.read_channel(r['id'])
    assert ch['close_requested_at'] and ch['grace_ends_at'] == ch['close_requested_at'] + CLOSE_GRACE_S
    with pytest.raises(ChannelError, match="The 7-day grace period after the close request hasn't ended yet."):
        chans.withdraw(keys['buyer_id'].key.to_0x_hex(), r['id'])

    # 8 days on: the identity withdraws, the rest goes back to the vault (the payer)
    fork.w3.provider.make_request('evm_increaseTime', [8 * 86400])
    fork.w3.provider.make_request('evm_mine', [])
    chans.withdraw(keys['buyer_id'].key.to_0x_hex(), r['id'])
    assert chans.read_channel(r['id'])['closed'] is True
    assert _balance(fork, keys['buyer_vault'].address) == 5 * SATORI - 3 * price


HUB = '0x1eec7E1443C1065ab78180A14326e5Ac8284f143'  # SatoriHub on Base Sepolia (Lite's base_config)


def test_a_staked_vault_cant_fund_a_channel_said_in_plain_words(fork):
    """A vault staked with its neuron (delegatePredictionPower to the identity): SATORI's transfer lock."""
    from eth_account import Account
    from satorilib.chain.channels import HOOK_GAS, ChannelError, PaymentChannels

    vault, identity = Account.create(), Account.create()
    _fund(fork, vault.address, satori=SATORI)
    hub = fork.contract(HUB, [{'name': 'delegatePredictionPower', 'type': 'function', 'stateMutability': 'nonpayable',
                               'inputs': [{'name': 'delegate', 'type': 'address'}], 'outputs': []}])
    fork.send(hub.functions.delegatePredictionPower(identity.address), private_key=vault.key.to_0x_hex(),
              gas_pad=HOOK_GAS)
    chans = PaymentChannels(fork, 84532, TOKEN)
    assert chans._lock_state(vault.address)[0] == 2 ** 24 - 1
    before = fork.w3.eth.get_transaction_count(vault.address)
    with pytest.raises(ChannelError, match="Your vault is staked with this neuron, so it can't send SATORI: "
                                           "a channel needs SATORI the vault can send."):
        chans.open_channel(vault.key.to_0x_hex(), identity.address, Account.create().address, 10 ** 15)
    assert fork.w3.eth.get_transaction_count(vault.address) == before  # nothing sent
