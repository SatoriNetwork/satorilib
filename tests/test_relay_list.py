"""getRelays asks central for the old list, or with a Nostr key for this neuron's own."""

from types import SimpleNamespace

from satorilib.server.server import SatoriServerClient


def _client(answer):
    asked = []
    client = SatoriServerClient.__new__(SatoriServerClient)

    def call(function, endpoint, **kwargs):
        asked.append(endpoint)
        return SimpleNamespace(json=lambda: answer)

    client._makeAuthenticatedCall = call
    return client, asked


def test_without_a_key_it_asks_for_the_list_every_neuron_gets():
    client, asked = _client([{"relay_url": "wss://a.example"}])
    assert client.getRelays() == [{"relay_url": "wss://a.example"}]
    assert asked == ["/api/v1/peer/relays"]


def test_with_its_nostr_key_it_asks_for_its_own_list():
    client, asked = _client([])
    assert client.getRelays(forPubkey="ab" * 32) == []
    assert asked == ["/api/v1/peer/relays?for=" + "ab" * 32]


def test_relay_config_is_asked_with_the_neurons_key(monkeypatch):
    from satorilib.server import server
    asked = []

    def get(url, params=None, timeout=None):
        asked.append((url, params))
        return SimpleNamespace(ok=True, json=lambda: {'keep': 2, 'streams': [{'name': 'bitcoin', 'cadence_seconds': 3600}]})
    monkeypatch.setattr(server.requests, 'get', get)
    client = SatoriServerClient.__new__(SatoriServerClient)
    client.url = 'https://central.example'
    assert client.getRelayConfig(forPubkey='ab' * 32)['streams'][0]['name'] == 'bitcoin'
    assert client.getRelayConfig() == client.getRelayConfig(forPubkey=None)
    assert asked == [('https://central.example/api/v1/peer/relay-config', {'for': 'ab' * 32}),
                     ('https://central.example/api/v1/peer/relay-config', None),
                     ('https://central.example/api/v1/peer/relay-config', None)]
