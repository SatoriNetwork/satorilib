"""EvmClient picks each transaction's nonce so back-to-back sends never reuse
one, even when a load-balanced RPC still reports the count from before the
last send (central's root publish then mintDevFee hit "nonce too low")."""

from types import SimpleNamespace

from satorilib.chain.evm import EvmClient


class _Eth:
    def __init__(self, count):
        self.count = count  # what the (lagging) node answers, every time
        self.sent = []
        self.gas_price = 1
        self.chain_id = 84532
        self.account = SimpleNamespace(from_key=lambda key: _Account())

    def get_transaction_count(self, address, block="latest"):
        return self.count

    def estimate_gas(self, tx):
        return 21000

    def send_raw_transaction(self, raw):
        self.sent.append(raw)
        return bytes([len(self.sent)])

    def wait_for_transaction_receipt(self, txhash):
        return SimpleNamespace(status=1)


class _Account:
    address = "0x9d3109b8A045D63056Aa5C1d2c883F3F9fA18817"

    def sign_transaction(self, tx):
        return SimpleNamespace(raw_transaction=tx["nonce"])


def _client(count):
    return EvmClient("http://stub", private_key="0x01", w3=SimpleNamespace(eth=_Eth(count)))


TO = "0x1efF47bc3a10a45D4B230B5d10E37751FE6AA718"


def test_back_to_back_sends_use_consecutive_nonces_on_a_lagging_node():
    c = _client(1040)
    c.send(to=TO, data="0x")
    c.send(to=TO, data="0x")
    assert c.w3.eth.sent == [1040, 1041]


def test_the_node_wins_when_it_is_ahead():
    c = _client(1040)
    c.send(to=TO, data="0x")
    c.w3.eth.count = 1050  # sent from elsewhere meanwhile
    c.send(to=TO, data="0x")
    assert c.w3.eth.sent == [1040, 1050]
