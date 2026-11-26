# Satori Nostr Relay

Run a Nostr relay for the Satori Network and earn a share of each day's reward.

This bundle runs a [strfry](https://github.com/hoytech/strfry) relay behind nginx. nginx also serves the relay's NIP-11 document, whose `pubkey` is the Nostr key that rewards are paid to.

```
Satori central ──fetch NIP-11──→ nginx ──→ nip11.json (pubkey = your relay key)
Nostr clients  ──WebSocket────→ nginx ──→ strfry
```

## Setup

You need a Linux VPS with Docker and a public IP.

1. Pick the relay's key. Any Nostr keypair you hold works: a new one, or your neuron's (Settings, "Run your own relay" shows it). Keep the private key; it is what spends the rewards.

2. Configure and start:

```bash
cp .env.example .env      # set NOSTR_PUBKEY (64 lowercase hex characters)
docker compose up -d
```

3. Check it from another machine:

```bash
curl -H "Accept: application/nostr+json" http://YOUR_IP:7777
```

You should see your key as `pubkey`.

Plain `ws://YOUR_IP:7777` works. For `wss://your-domain`, put Caddy or nginx in front for TLS. The address must be public: central does not connect to private ones.

## Getting found

There is nothing to register. Central builds its relay list from the relay lists (NIP-65) that registered neurons publish. On a neuron that predicts, open Settings, External Relay Connections, and add your relay URL. A predicting neuron stays connected to every relay it knows, announces its prediction streams there, and lists them in its relay list, which it republishes daily. Central picks the relay up at its next daily reload.

## How you are paid

- Central keeps one connection to each relay it knows. Time counts only while the relay holds at least one Satori stream announcement, and messages count only when written by registered neurons.
- Every relay connected 20 hours or more on a UTC day shares that day's relay pool (0.1% of the daily off-chain reward pool) equally. Message counts are published in the audit (`https://network.satorinet.io/api/v1/audit/relays/latest`) but do not change the amount today.
- The reward goes to the Base address of `02 || NOSTR_PUBKEY` (the even-y convention). To spend it, open the Key Normalization Tool on a neuron's Settings page, enter the relay's private key, and import the normalized key into Satori GO, MetaMask or any Base wallet. About half of all keys need this step so the wallet address matches.

## Capacity

The bundle sets its own connection limits (`nginx.conf`, and `ulimits` in `docker-compose.yml`), so there is nothing to tune. Measured on a 4 core machine: one relay held 40,000 open connections using about 1.4 GB of memory (about 35 KB each). That is connections held open, not traffic. With the stock nginx limits the same machine stopped at about 1,900.

If you put Caddy or nginx in front for TLS, that proxy needs room for the same number of connections (in nginx: `worker_connections` and `worker_rlimit_nofile`).

After changing `strfry.conf` or `.env`, `docker compose up -d --build` is enough: nginx finds the new strfry container by itself.

## Configuration

| Variable | Required | Description |
|----------|----------|-------------|
| `NOSTR_PUBKEY` | Yes | The relay's Nostr public key (64 lowercase hex characters); rewards go to its Base address |
| `RELAY_NAME` | No | Relay display name (default: "Satori Relay") |
| `RELAY_DESCRIPTION` | No | Relay description |
| `RELAY_CONTACT` | No | Admin contact (email or URL) |
| `RELAY_PORT` | No | Port to expose (default: 7777) |

## Architecture

```
docker compose
├── init       (writes nip11.json from NOSTR_PUBKEY, runs once)
├── strfry     (the relay, port 7777 inside the compose network)
└── nginx      (serves NIP-11, proxies the WebSocket, published on RELAY_PORT)
```
