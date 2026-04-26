# satorilib

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

`satorilib` is the shared Python library that underpins the
[Satori Network](https://github.com/SatoriNetwork). It provides the wallet
stack, central-server client, datastream concepts, persistence helpers, and
transport adapters that other Satori components — including the
[satori-lite](https://github.com/SatoriNetwork/satori-lite) neuron — depend
on.

The library is intended to be consumed as a dependency rather than run on its
own. It is organized as a collection of focused, independently importable
subpackages so that downstream applications can pull in only what they need.

## Capabilities

- **Wallets and identity** (`satorilib.wallet`)
  Multi-chain wallets for Evrmore, Ravencoin, and Ethereum, with key
  derivation, signing, and identity primitives.
- **Server client** (`satorilib.server`)
  Authenticated HTTP client for the Satori central server: checkin,
  balances, stream and subscription management, and OFAC compliance helpers.
- **Domain concepts** (`satorilib.concepts`)
  Canonical data structures for streams, observations, identifiers, and
  related domain objects shared across the network.
- **Datastream pub/sub over Nostr** (`satorilib.satori_nostr`)
  End-to-end encrypted datastream announcement, subscription, payment, and
  delivery primitives built on Nostr, including production-ready
  reliability integrations.
- **Additional transports**
  Centrifugo client (`satorilib.centrifugo`), websockets, ZeroMQ, FTP,
  gossip, IPFS, and a generic pubsub layer.
- **Payment channels** (`satorilib.payment_channel`)
  Building blocks for off-chain micropayment flows.
- **Persistence and I/O** (`satorilib.disk`, `satorilib.sqlite`)
  File and cache helpers and SQLite utilities used across components.
- **Asynchronous helpers** (`satorilib.asynchronous`)
  Thread, task, and event-loop utilities used by long-running services.
- **Electrum and RPC clients** (`satorilib.electrumx`, `satorilib.rpc`)
  Lightweight clients for blockchain node interaction.
- **Validation, logging, configuration, utilities**
  Cross-cutting helpers used by the rest of the library.

## Installation

Install directly from the GitHub repository:

```bash
pip install git+https://github.com/SatoriNetwork/satorilib.git
```

Or, for development, clone the repository and install in editable mode:

```bash
git clone https://github.com/SatoriNetwork/satorilib.git
cd satorilib
pip install -e .
```

Python 3.8 or newer is required. Some subpackages depend on system
libraries such as `libsecp256k1` and `libssl`; install the corresponding
development headers when building cryptography dependencies from source.

## Usage

Subpackages are imported individually so that pulling in `satorilib` does
not require initializing transports or backends that an application does
not use. A few representative examples:

```python
# Evrmore wallet and identity
from satorilib.wallet import EvrmoreWallet
from satorilib.wallet.evrmore.identity import EvrmoreIdentity

# Central-server client
from satorilib.server import SatoriServerClient

# Stream domain objects
from satorilib.concepts.structs import StreamId, Stream

# Datastream pub/sub over Nostr
from satorilib.satori_nostr import SatoriNostr, SatoriNostrConfig
```

## Project Layout

```
src/satorilib/
  asynchronous/      Thread and async helpers
  centrifugo/        Centrifugo client
  concepts/          Stream, observation, and identifier types
  config/            Configuration helpers
  datamanager/       Data acquisition and management primitives
  disk/              Filesystem and cache utilities
  electrumx/         ElectrumX client
  experimental/      Experimental modules (unstable APIs)
  ftp/, ipfs/        Optional transport backends
  gossip/            Gossip-style messaging primitives
  interfaces/        Shared interface definitions
  logging/           Logging configuration
  payment_channel/   Off-chain payment channel primitives
  pubsub/            Generic publish/subscribe layer
  relay/             Relay support utilities
  rpc/               JSON-RPC client helpers
  satori_nostr/      Datastream pub/sub and micropayments over Nostr
  server/            Satori central-server client
  sqlite/            SQLite helpers
  utils/             Cross-cutting utilities
  validate/          Validation helpers
  wallet/            Multi-chain wallet stack (Evrmore, Ravencoin, Ethereum)
  websockets/        WebSocket transport
  zeromq/            ZeroMQ transport
```

## Testing

The repository includes a `tests/` suite. Run it with:

```bash
pytest tests/
```

Some tests target specific subpackages and can be selected by path or
marker; see the suite for details.

## Compatibility

`satorilib` is the canonical home for primitives shared across Satori
components. When integrating with the lightweight neuron build in
[satori-lite](https://github.com/SatoriNetwork/satori-lite), install the
two repositories into the same environment so the neuron can import
`satorilib` directly.

## Contributing

Issues and pull requests are welcome. When proposing changes that affect
public APIs used by other Satori components, please open an issue first to
coordinate the change and avoid breaking downstream consumers.

## License

`satorilib` is released under the MIT License. See `LICENSE` for the full
text.
