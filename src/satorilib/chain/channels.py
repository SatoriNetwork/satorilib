"""Payment channels on Base: the PaymentChannels contract as the paid-stream
market uses it, the same as the browser neuron (Satori/satori
src/chain/channels.js; the contract's rules: Satori/contract
docs/design/payment-channels.md).

The wallet funds a channel (`open_channel`, with SATORI's permit); the alias
signs its vouchers and the binding, asks to close and withdraws (the refund
goes to the wallet, the payer); a seller's alias settles into the seller's
wallet (`settle_many`). Amounts are int wei; keys are 0x private keys.

    chans = PaymentChannels(EvmClient(rpc), chain_id, token)
    chans.open_channel(wallet_key, alias_address, payee, deposit) → {id, hash, permit}
    sign_voucher(alias_key, chain_id, channel_id, cumulative) / verify_voucher(signer, sig, …)
    chans.settle_many(alias_key, [{id, cumulative, signature}]) → {hash, settled, skipped}
    chans.request_close(alias_key, id) … CLOSE_GRACE_S … chans.withdraw(alias_key, id)
"""
import os
import time
from datetime import datetime, timezone

from eth_abi import encode
from eth_account import Account
from eth_account.messages import encode_defunct, encode_typed_data
from eth_utils import keccak, to_checksum_address

# PaymentChannels, deployed by CREATE2: the same address on every chain. Not central's to say.
ADDRESS = '0xeF3f87042cd9619729E5D69Da2fF90F89EA6976c'
CLOSE_GRACE_S = 7 * 86400  # CLOSE_GRACE_PERIOD: requestClose → withdraw
MAX_AMOUNT = 2 ** 96 - 1  # amounts are uint96 wei
# Gas over the estimate: a payment into a staked wallet runs the token's delegation
# hook, and a starved hook is skipped silently on D6 (the contract doc). settleMany
# adds PAYMENT_GAS per payment (measured: ~100,000 the first time, 70,000 after).
HOOK_GAS = 500_000
PAYMENT_GAS = 100_000
PERMIT_S = 3600  # a permit's deadline: chain time + 1 h
ZERO = '0x' + '0' * 40
_N = 0xfffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141  # secp256k1's order
_HALF_N = _N >> 1  # OpenZeppelin's ECDSA refuses s above it (the malleable twin)
_INDEFINITE = 2 ** 24 - 1  # transferLockedUntil while staked
_ROUND_S = 86400

_CHANNEL = [
    {'name': 'payer', 'type': 'address'}, {'name': 'deposit', 'type': 'uint96'},
    {'name': 'payee', 'type': 'address'}, {'name': 'settled', 'type': 'uint96'},
    {'name': 'token', 'type': 'address'}, {'name': 'closeRequestedAt', 'type': 'uint64'},
    {'name': 'closed', 'type': 'bool'}, {'name': 'signer', 'type': 'address'},
]


def _fn(name, inputs, outputs=(), view=False):
    return {'name': name, 'type': 'function', 'stateMutability': 'view' if view else 'nonpayable',
            'inputs': [{'name': n, 'type': t} for n, t in inputs], 'outputs': list(outputs)}


CHANNELS_ABI = [
    _fn('open', [('payer', 'address'), ('payee', 'address'), ('token', 'address'), ('signer', 'address'),
                 ('deposit', 'uint96'), ('salt', 'bytes32')], [{'name': '', 'type': 'bytes32'}]),
    _fn('openWithPermit', [('payer', 'address'), ('payee', 'address'), ('token', 'address'), ('signer', 'address'),
                           ('deposit', 'uint96'), ('salt', 'bytes32'), ('deadline', 'uint256'), ('v', 'uint8'),
                           ('r', 'bytes32'), ('s', 'bytes32')], [{'name': '', 'type': 'bytes32'}]),
    _fn('settleMany', [('channelIds', 'bytes32[]'), ('cumulativeAmounts', 'uint96[]'), ('signatures', 'bytes[]')],
        [{'name': 'settledCount', 'type': 'uint256'}]),
    _fn('requestClose', [('channelId', 'bytes32')]),
    _fn('withdraw', [('channelId', 'bytes32')]),
    {'name': 'getChannel', 'type': 'function', 'stateMutability': 'view', 'inputs': [{'name': 'channelId', 'type': 'bytes32'}],
     'outputs': [{'name': '', 'type': 'tuple', 'components': _CHANNEL}]},
    {'name': 'Settled', 'type': 'event', 'anonymous': False, 'inputs': [
        {'name': 'channelId', 'type': 'bytes32', 'indexed': True}, {'name': 'payer', 'type': 'address', 'indexed': True},
        {'name': 'payee', 'type': 'address', 'indexed': True}, {'name': 'cumulativeAmount', 'type': 'uint96', 'indexed': False},
        {'name': 'paid', 'type': 'uint96', 'indexed': False}, {'name': 'settled', 'type': 'uint96', 'indexed': False}]},
]

TOKEN_ABI = [
    _fn('approve', [('spender', 'address'), ('value', 'uint256')], [{'name': '', 'type': 'bool'}]),
    _fn('allowance', [('owner', 'address'), ('spender', 'address')], [{'name': '', 'type': 'uint256'}], view=True),
    _fn('nonces', [('owner', 'address')], [{'name': '', 'type': 'uint256'}], view=True),
    _fn('name', [], [{'name': '', 'type': 'string'}], view=True),
    _fn('DOMAIN_SEPARATOR', [], [{'name': '', 'type': 'bytes32'}], view=True),
    _fn('eip712Domain', [], [{'name': 'fields', 'type': 'bytes1'}, {'name': 'name', 'type': 'string'},
                             {'name': 'version', 'type': 'string'}, {'name': 'chainId', 'type': 'uint256'},
                             {'name': 'verifyingContract', 'type': 'address'}, {'name': 'salt', 'type': 'bytes32'},
                             {'name': 'extensions', 'type': 'uint256[]'}], view=True),
    _fn('transferLockedUntil', [('user', 'address')], [{'name': '', 'type': 'uint24'}], view=True),
    _fn('getCurrentRound', [], [{'name': '', 'type': 'uint256'}], view=True),
]


class ChannelError(Exception):
    """An error in plain words; `reason` is the contract's error name (or ours)."""

    def __init__(self, message, reason=None):
        super().__init__(message)
        self.reason = reason


PLAIN = {
    'BadInput': "The channel's addresses or amount are not valid.",
    'ChannelExists': 'A channel with this id already exists: open it with a new salt.',
    'UnknownChannel': 'There is no such channel.',
    'ChannelClosed': 'The channel is closed.',
    'NotPayee': "Only the seller (the channel's payee) can do that.",
    'NotPayer': 'Only the wallet that funded the channel can do that.',
    'NotPayerOrSigner': "Only the channel's wallet or alias can do that.",
    'BadSignature': "The voucher's signature isn't the channel's signer's.",
    'NothingToSettle': 'Nothing new to settle: the voucher pays no more than is already paid.',
    'CloseNotRequested': 'No close was asked for this channel.',
    'CloseNotReady': "The 7-day grace period after the close request hasn't ended yet.",
    'UnsupportedToken': "The token takes a fee on transfer, which channels can't hold.",
    'SafeERC20FailedOperation': 'The token refused the transfer.',
    'Locked': "The wallet's SATORI can't be sent now: it is staked, or it predicted by hand this round or the last.",
    'ERC20InsufficientBalance': 'Not enough SATORI in the wallet.',
    'ERC20InsufficientAllowance': "The channel contract wasn't allowed to take the deposit.",
    'HookFailedWithoutReason': 'The transaction ran out of gas; try again.',
}
_ERRORS = ['BadInput()', 'ChannelExists()', 'UnknownChannel()', 'ChannelClosed()', 'NotPayee()', 'NotPayer()',
           'NotPayerOrSigner()', 'BadSignature()', 'NothingToSettle()', 'CloseNotRequested()', 'CloseNotReady()',
           'UnsupportedToken()', 'SafeERC20FailedOperation(address)', 'Locked()',
           'ERC20InsufficientBalance(address,uint256,uint256)', 'ERC20InsufficientAllowance(address,uint256,uint256)',
           'HookFailedWithoutReason()']
_SELECTORS = {keccak(text=sig)[:4].hex(): sig.split('(')[0] for sig in _ERRORS}


def revert_name(err):
    """The contract's (or the token's) custom error name in a web3 error, or None."""
    for x in [getattr(err, 'data', None), *getattr(err, 'args', ())]:
        if isinstance(x, (bytes, bytearray)):
            x = '0x' + bytes(x).hex()
        if isinstance(x, str) and x.startswith('0x') and len(x) >= 10:
            name = _SELECTORS.get(x[2:10].lower())
            if name:
                return name
    return None


def channel_error_text(err) -> str:
    """Any error in a line: the contract's reason in plain words, else the error's own message."""
    if isinstance(err, ChannelError):
        return str(err)
    return PLAIN.get(revert_name(err)) or str(err)


def _plain(err):
    return err if isinstance(err, ChannelError) else ChannelError(channel_error_text(err), reason=revert_name(err))


def _amount(x, what='The amount', positive=False) -> int:
    if not isinstance(x, int) or isinstance(x, bool) or x < (1 if positive else 0) or x > MAX_AMOUNT:
        raise ChannelError(f'{what} must be an int of wei from {1 if positive else 0} to 2^96 − 1.', reason='BadInput')
    return x


def _b32(h) -> bytes:
    b = bytes(h) if isinstance(h, (bytes, bytearray)) else bytes.fromhex(str(h).removeprefix('0x'))
    if len(b) != 32:
        raise ChannelError('A channel id is 32 bytes.', reason='BadInput')
    return b


def _hex(b) -> str:
    return '0x' + bytes(b).hex()


def same(a, b) -> bool:
    return bool(a) and bool(b) and str(a).lower() == str(b).lower()


def channel_id_of(payer, payee, token, signer, salt) -> str:
    """keccak256(abi.encode(payer, payee, token, signer, salt)): the contract's computeChannelId."""
    return _hex(keccak(encode(['address', 'address', 'address', 'address', 'bytes32'],
                              [to_checksum_address(payer), to_checksum_address(payee), to_checksum_address(token),
                               to_checksum_address(signer), _b32(salt)])))


def new_salt() -> str:
    """32 random bytes: a channel's id is never reused, even after it closes."""
    return _hex(os.urandom(32))


def voucher_typed_data(chain_id, channel_id, cumulative, contract=ADDRESS) -> dict:
    """The EIP-712 voucher "channel `channel_id` owes its payee `cumulative` in all", under the contract's domain."""
    return {
        'types': {
            'EIP712Domain': [{'name': 'name', 'type': 'string'}, {'name': 'version', 'type': 'string'},
                             {'name': 'chainId', 'type': 'uint256'}, {'name': 'verifyingContract', 'type': 'address'}],
            'Voucher': [{'name': 'channelId', 'type': 'bytes32'}, {'name': 'cumulativeAmount', 'type': 'uint96'}],
        },
        'primaryType': 'Voucher',
        'domain': {'name': 'PaymentChannels', 'version': '1', 'chainId': int(chain_id),
                   'verifyingContract': to_checksum_address(contract)},
        'message': {'channelId': _b32(channel_id), 'cumulativeAmount': _amount(cumulative)},
    }


def _digest(signable) -> str:
    return _hex(keccak(b'\x19' + signable.version + signable.header + signable.body))


def voucher_digest(chain_id, channel_id, cumulative, contract=ADDRESS) -> str:
    """Its digest: the contract's voucherDigest(channelId, cumulative)."""
    return _digest(encode_typed_data(full_message=voucher_typed_data(chain_id, channel_id, cumulative, contract)))


def sign_voucher(private_key, chain_id, channel_id, cumulative, contract=ADDRESS) -> str:
    """A voucher signed by the alias's key: 65 bytes, 0x hex. No gas."""
    signed = Account.sign_message(
        encode_typed_data(full_message=voucher_typed_data(chain_id, channel_id, cumulative, contract)), private_key)
    return _hex(signed.signature)


def _ecdsa_shape(signature) -> bool:
    """What OpenZeppelin 5's ECDSA.tryRecover(hash, bytes) accepts: 65 bytes, v 27 or 28, 0 < r < n, 0 < s ≤ n/2."""
    if not isinstance(signature, str) or len(signature) != 132 or not signature.startswith('0x'):
        return False
    try:
        r, s, v = int(signature[2:66], 16), int(signature[66:130], 16), int(signature[130:], 16)
    except ValueError:
        return False
    return v in (27, 28) and 0 < r < _N and 0 < s <= _HALF_N


def recover_voucher_signer(signature, chain_id, channel_id, cumulative, contract=ADDRESS):
    """Who signed a voucher, by ECDSA alone and under the contract's rules; None when the contract's ECDSA
    would refuse it. A seller accepts only key signers: a contract's ERC-1271 answer can change."""
    if not _ecdsa_shape(signature):
        return None
    try:
        return Account.recover_message(
            encode_typed_data(full_message=voucher_typed_data(chain_id, channel_id, cumulative, contract)),
            signature=bytes.fromhex(signature[2:]))
    except Exception:
        return None


def verify_voucher(signer, signature, chain_id, channel_id, cumulative, contract=ADDRESS) -> bool:
    """Whether `signer` signed this voucher for this channel, amount, chain and contract, as the contract checks it."""
    return same(recover_voucher_signer(signature, chain_id, channel_id, cumulative, contract), signer)


def due_of(deposit, settled, cumulative) -> int:
    """What `cumulative` would pay the payee now: min(cumulative, deposit) − settled, never below 0."""
    target = min(cumulative, deposit)
    return target - settled if target > settled else 0


def binding_text(channel_id, nostr_pubkey) -> str:
    """What the alias signs to say this Nostr key may use the channel (EIP-191)."""
    return f'satori-market:v1:{str(channel_id).lower()}:{nostr_pubkey}'


def sign_binding(private_key, channel_id, nostr_pubkey) -> str:
    """The alias's binding: its personal_sign of binding_text, 0x hex."""
    return _hex(Account.sign_message(encode_defunct(text=binding_text(channel_id, nostr_pubkey)), private_key).signature)


def binding_signer(channel_id, nostr_pubkey, binding):
    """Who signed a binding, or None."""
    try:
        return Account.recover_message(encode_defunct(text=binding_text(channel_id, nostr_pubkey)),
                                       signature=bytes.fromhex(str(binding).removeprefix('0x')))
    except Exception:
        return None


def transfer_lock(locked_until, round_) -> dict:
    """SATORI's transfer rule: blocked while `locked_until != 0 and locked_until >= round`; free from round
    locked_until + 1, at its 00:00 UTC."""
    locked = locked_until != 0 and locked_until >= round_
    indefinite = locked and locked_until >= _INDEFINITE
    finite = locked and not indefinite
    return {'locked': locked, 'indefinite': indefinite, 'until_round': locked_until if finite else None,
            'free_at': (locked_until + 1) * _ROUND_S if finite else None}


def _fmt_utc(s) -> str:
    d = datetime.fromtimestamp(s, timezone.utc)
    return f'{d:%b} {d.day}, {d:%H:%M} UTC'


def lock_error(lock):
    """SATORI's transfer lock in plain words (a funded channel needs SATORI the wallet can send), or None."""
    if not lock['locked']:
        return None
    why = ("Your wallet is staked with this neuron, so it can't send SATORI" if lock['indefinite']
           else f"Your wallet's SATORI can't be sent until {_fmt_utc(lock['free_at'])} (round {lock['until_round'] + 1})")
    return ChannelError(f'{why}: a channel needs SATORI the wallet can send.', reason='Locked')


class PaymentChannels:
    """The channel contract's reads and writes over satorilib's EvmClient. Writes are padded by HOOK_GAS
    (settleMany by PAYMENT_GAS more per payment) and raise ChannelError in plain words."""

    def __init__(self, client, chain_id, token, address=ADDRESS, settle_tries=5, settle_wait=1.0):
        self.client = client
        self.chain_id = int(chain_id)
        self.token = token
        self.address = address
        self.contract = client.contract(address, CHANNELS_ABI)
        self._settle = (settle_tries, settle_wait)

    def _erc20(self):
        return self.client.contract(self.token, TOKEN_ABI)

    def chain_time(self) -> int:
        return self.client.latest_timestamp()

    def read_channel(self, channel_id):
        """A channel as the contract holds it (amounts int wei, times unix seconds), or None for an id never opened."""
        try:
            payer, deposit, payee, settled, token, asked, closed, signer = self.contract.functions.getChannel(
                _b32(channel_id)).call()
        except Exception as e:
            raise _plain(e) from e
        if int(payer, 16) == 0:
            return None
        asked = int(asked)
        return {'id': channel_id, 'payer': payer, 'payee': payee, 'token': token, 'signer': signer,
                'deposit': int(deposit), 'settled': int(settled), 'close_requested_at': asked or None,
                'closed': bool(closed), 'grace_ends_at': asked + CLOSE_GRACE_S if asked else None}

    def _lock_state(self, wallet):
        t = self._erc20()
        return (int(t.functions.transferLockedUntil(to_checksum_address(wallet)).call()),
                int(t.functions.getCurrentRound().call()))

    def _permit(self, account, value):
        """An ERC-2612 permit for `value` to the channel contract, signed by the wallet; None when the token has
        none. The domain is ERC-5267's eip712Domain(), else name() and version "1", and only if it hashes to
        the token's DOMAIN_SEPARATOR()."""
        t = self._erc20()
        try:
            nonce = int(t.functions.nonces(account.address).call())
            separator = bytes(t.functions.DOMAIN_SEPARATOR().call())
        except Exception:
            return None
        domain_types, domain = [], {}
        try:
            fields, name, version, chain_id, verifying, salt, _ = t.functions.eip712Domain().call()
            f = fields[0] if isinstance(fields, (bytes, bytearray)) else int(fields)
            for bit, key, typ, val in [(1, 'name', 'string', name), (2, 'version', 'string', version),
                                       (4, 'chainId', 'uint256', int(chain_id)),
                                       (8, 'verifyingContract', 'address', verifying), (16, 'salt', 'bytes32', salt)]:
                if f & bit:
                    domain_types.append({'name': key, 'type': typ})
                    domain[key] = val
        except Exception:
            try:
                name = t.functions.name().call()
            except Exception:
                return None
            domain_types = [{'name': 'name', 'type': 'string'}, {'name': 'version', 'type': 'string'},
                            {'name': 'chainId', 'type': 'uint256'}, {'name': 'verifyingContract', 'type': 'address'}]
            domain = {'name': name, 'version': '1', 'chainId': self.chain_id, 'verifyingContract': self.token}
        deadline = self.chain_time() + PERMIT_S
        message = {
            'types': {'EIP712Domain': domain_types,
                      'Permit': [{'name': 'owner', 'type': 'address'}, {'name': 'spender', 'type': 'address'},
                                 {'name': 'value', 'type': 'uint256'}, {'name': 'nonce', 'type': 'uint256'},
                                 {'name': 'deadline', 'type': 'uint256'}]},
            'primaryType': 'Permit', 'domain': domain,
            'message': {'owner': account.address, 'spender': to_checksum_address(self.address), 'value': value,
                        'nonce': nonce, 'deadline': deadline}}
        signable = encode_typed_data(full_message=message)
        if bytes(signable.header) != separator:
            return None
        signed = account.sign_message(signable)
        return {'deadline': deadline, 'v': signed.v, 'r': signed.r.to_bytes(32, 'big'), 's': signed.s.to_bytes(32, 'big')}

    def _approve(self, account, value):
        """approve(channels, value) when the allowance is short, then wait until the RPC shows it. → its hash, or None"""
        t = self._erc20()
        spender = to_checksum_address(self.address)
        allowance = lambda: int(t.functions.allowance(account.address, spender).call())
        if allowance() >= value:
            return None
        h = self.client.send(t.functions.approve(spender, value), private_key=account.key, gas_pad=HOOK_GAS)
        tries, wait = self._settle
        for _ in range(tries):
            try:
                if allowance() >= value:
                    break
            except Exception:
                pass
            time.sleep(wait)
        return h

    def open_channel(self, wallet_key, alias_address, payee, deposit, salt=None) -> dict:
        """The wallet opens a channel to `payee` (the seller's wallet) with `deposit` of SATORI: payer = the wallet,
        signer = the alias. SATORI's transfer lock first, before anything is signed; then one openWithPermit,
        or for a token without a permit approve, then open. → {id, hash, permit, approve?}"""
        try:
            _amount(deposit, 'The deposit', positive=True)
            account = Account.from_key(wallet_key)
            payer, signer = account.address, to_checksum_address(alias_address)
            payee, token = to_checksum_address(payee), to_checksum_address(self.token)
            salt = salt or new_salt()
            cid = channel_id_of(payer, payee, token, signer, salt)
            err = lock_error(transfer_lock(*self._lock_state(payer)))
            if err:
                raise err
            permit = self._permit(account, deposit)
            fns = self.contract.functions
            if permit:
                h = self.client.send(fns.openWithPermit(payer, payee, token, signer, deposit, _b32(salt), permit['deadline'],
                                                        permit['v'], permit['r'], permit['s']),
                                     private_key=wallet_key, gas_pad=HOOK_GAS)
                return {'id': cid, 'hash': h, 'permit': True}
            approve = self._approve(account, deposit)
            h = self.client.send(fns.open(payer, payee, token, signer, deposit, _b32(salt)),
                                 private_key=wallet_key, gas_pad=HOOK_GAS)
            return {'id': cid, 'hash': h, 'permit': False, **({'approve': approve} if approve else {})}
        except Exception as e:
            raise _plain(e) from e

    def request_close(self, alias_key, channel_id) -> str:
        """The alias starts the 7-day grace period; asking again doesn't restart it."""
        try:
            return self.client.send(self.contract.functions.requestClose(_b32(channel_id)),
                                    private_key=alias_key, gas_pad=HOOK_GAS)
        except Exception as e:
            raise _plain(e) from e

    def withdraw(self, alias_key, channel_id) -> str:
        """After the grace period, the alias sends what was not settled back to the wallet (the payer)."""
        try:
            return self.client.send(self.contract.functions.withdraw(_b32(channel_id)),
                                    private_key=alias_key, gas_pad=HOOK_GAS)
        except Exception as e:
            raise _plain(e) from e

    def settle_many(self, alias_key, entries) -> dict:
        """The seller's alias settles many vouchers in one transaction ([{id, cumulative, signature}]), only
        those that would pay: the rest come back in `skipped` ({id, reason: 'unknown' | 'closed' | 'nothing-due'
        | 'bad-signature'}); with none left nothing is sent. → {hash, settled: [{id, cumulative, paid, settled}], skipped}"""
        try:
            keep, skipped = [], []
            for e in entries:
                _amount(e['cumulative'])
                c = self.read_channel(e['id'])
                reason = ('unknown' if c is None else 'closed' if c['closed']
                          else 'nothing-due' if due_of(c['deposit'], c['settled'], e['cumulative']) == 0
                          else 'bad-signature' if not verify_voucher(c['signer'], e['signature'], self.chain_id, e['id'],
                                                                     e['cumulative'], self.address)
                          else None)
                if reason:
                    skipped.append({'id': e['id'], 'reason': reason})
                else:
                    keep.append(e)
            if not keep:
                return {'hash': None, 'settled': [], 'skipped': skipped}
            h = self.client.send(self.contract.functions.settleMany(
                [_b32(e['id']) for e in keep], [e['cumulative'] for e in keep],
                [bytes.fromhex(e['signature'][2:]) for e in keep]),
                private_key=alias_key, gas_pad=HOOK_GAS + len(keep) * PAYMENT_GAS)
            return {'hash': h, 'settled': self._settled_of(h), 'skipped': skipped}
        except Exception as e:
            raise _plain(e) from e

    def _settled_of(self, tx_hash):
        """The Settled events of a transaction, this contract's only."""
        from web3.logs import DISCARD
        receipt = self.client.w3.eth.get_transaction_receipt(tx_hash)
        logs = [l for l in receipt['logs'] if same(l.get('address'), self.address)]
        return [{'id': _hex(ev['args']['channelId']), 'cumulative': int(ev['args']['cumulativeAmount']),
                 'paid': int(ev['args']['paid']), 'settled': int(ev['args']['settled'])}
                for ev in self.contract.events.Settled().process_receipt({'logs': logs}, errors=DISCARD)]
