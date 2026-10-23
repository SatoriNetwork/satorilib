"""Minimal EVM chain client shared by Satori's central server and the neuron.

One home for the web3 plumbing — connect to an RPC, read (call), and write
(build → sign → send → wait) — so the signing/sending code isn't copied into
each repo's contract client. Contract-specific logic (which functions, which
ABIs, Merkle trees, claims, predictions) stays in the consumer; only the
generic transaction machinery lives here.

Deliberately NOT under `satorilib.wallet` (that package imports the native
Evrmore/Ravencoin libs); this imports only web3 + eth-account, lazily, so a
consumer that never touches those libs can use it without dragging them in.
"""

import logging

logger = logging.getLogger(__name__)


class EvmClient:
    """A thin web3 wrapper: read (`call_raw`, `contract().functions...call()`)
    and write (`send`) EVM contract functions with one shared, tested tx path.

    The signing key can be supplied at construction (`private_key`) or per
    `send`. `w3` can be injected for tests. web3 is imported lazily so importing
    this module needs no network and no web3 until first use."""

    def __init__(self, rpc_url: str, private_key: str = None, w3=None):
        self.rpc_url = rpc_url
        self._private_key = private_key
        self._w3 = w3
        # Next nonce per account after our own sends: a load-balanced RPC can
        # still report the count from before the last send ("nonce too low").
        self._next_nonce = {}

    @property
    def w3(self):
        if self._w3 is None:
            from web3 import Web3
            self._w3 = Web3(Web3.HTTPProvider(self.rpc_url))
        return self._w3

    def contract(self, address: str, abi):
        from web3 import Web3
        return self.w3.eth.contract(
            address=Web3.to_checksum_address(address), abi=abi)

    @property
    def chain_id(self) -> int:
        return self.w3.eth.chain_id

    def latest_timestamp(self) -> int:
        return int(self.w3.eth.get_block("latest")["timestamp"])

    def account_address(self, private_key: str = None) -> str:
        key = private_key or self._private_key
        return self.w3.eth.account.from_key(key).address

    def call_raw(self, to: str, data: str) -> bytes:
        """A raw eth_call (hand-built calldata). Returns the return bytes."""
        from web3 import Web3
        return self.w3.eth.call({"to": Web3.to_checksum_address(to), "data": data})

    def send(self, fn=None, private_key: str = None, to: str = None, data: str = None,
             gas_pad: int = 0) -> str:
        """Build → sign → send → wait for a transaction, returning the 0x tx
        hash. Provide either a contract function `fn` (its target/calldata come
        from it) OR a raw (`to`, `data`) pair. Signs with `private_key` or the
        client's key. `gas_pad` is added to the gas estimate: a SATORI payment
        into a staked wallet runs the token's hook, which a bare estimate can
        starve. Raises on missing key or on-chain revert."""
        key = private_key or self._private_key
        if not key:
            raise RuntimeError("no signing key for send")
        account = self.w3.eth.account.from_key(key)
        gas_price = self.w3.eth.gas_price
        nonce = max(self.w3.eth.get_transaction_count(account.address, "pending"),
                    self._next_nonce.get(account.address, 0))
        tx = {
            "from": account.address,
            "nonce": nonce,
            "chainId": self.w3.eth.chain_id,
            "maxFeePerGas": gas_price * 2,
            "maxPriorityFeePerGas": gas_price,
        }
        if fn is not None:
            tx = fn.build_transaction(tx)
        else:
            from web3 import Web3
            tx["to"] = Web3.to_checksum_address(to)
            tx["value"] = 0
            tx["data"] = data
            tx["gas"] = self.w3.eth.estimate_gas(tx)
        tx["gas"] += int(gas_pad)
        signed = account.sign_transaction(tx)
        txhash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
        self._next_nonce[account.address] = nonce + 1
        receipt = self.w3.eth.wait_for_transaction_receipt(txhash)
        h = txhash.hex()
        h = h if h.startswith("0x") else "0x" + h
        if receipt.status != 1:
            raise RuntimeError(f"transaction reverted: {h}")
        return h
