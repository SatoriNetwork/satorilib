# Payment Channel Management — Scratchpad

## The Problem

The wallet can **create** payment channels and **remember** them (via `wallet.scripts.json`). But after funding a channel and closing the wallet, there's no way to know what happened on-chain. Did the receiver claim? Is the timelock expired? Is the UTXO still sitting there? The wallet is blind to its own P2SH outputs.

For P2PKH, the wallet subscribes to its own scripthash via ElectrumX and gets notified of every balance change. Payment channels need the same treatment.

## Needs (full picture)

These are all the things that need to work for payment channels to be fully operational. Not all of them belong in satorilib — some are application-layer concerns. Listed here for context.

| # | Need | Layer | Notes |
|---|------|-------|-------|
| 1 | **On-chain monitoring** — subscribe to P2SH scripthashes, detect when funding UTXOs are spent | wallet (satorilib) | Same pattern as existing P2PKH subscription |
| 2 | **Timelock awareness** — know when recall window opens (CSV relative or CLTV absolute) | wallet (satorilib) | Derived from redeem script params + block height |
| 3 | **State machine** — formal lifecycle states (funded → active → spent/expired) | wallet (satorilib) | Driven by events from #1 and #2 |
| 4 | **Commitment tracking** — exchange off-chain commitments with counterparty | application | Uses `PaymentChannelClient` microservice + multisig tx methods |
| 5 | **Auto-claim / auto-recall** — policy decisions about when to claim or reclaim | application | Informed by #2 and #3, executed via wallet tx methods |

## Layer 1: ElectrumX P2SH Subscriptions (detailed)

### How it works today for P2PKH

`subscribeToScripthashActivity()` (wallet.py:351-379):
- Computes scripthash from P2PKH scriptPubKey: `OP_DUP OP_HASH160 <pubKeyHash> OP_EQUALVERIFY OP_CHECKSIG` → SHA256 → reverse bytes
- Calls `electrumx.api.subscribeScripthash(scripthash, callback)`
- Callback triggers: `getBalances()`, `getUnspents()`, `getUnspentTransactions()`, `saveCache()`
- Called by the application after wallet init + ElectrumX connection — NOT auto-triggered in `__init__`

### What's different for P2SH

The scripthash computation uses a different scriptPubKey format:
```
P2PKH: OP_DUP OP_HASH160 <pubKeyHash> OP_EQUALVERIFY OP_CHECKSIG  (76 a9 14 <20b> 88 ac)
P2SH:  OP_HASH160 <scriptHash> OP_EQUAL                            (a9 14 <20b> 87)
```

The P2SH address encodes the script hash (base58check with version byte). So from an address:
```python
script_hash = b58decode_check(p2shAddress)[1:]  # strip version byte
scriptPubKey = bytes.fromhex('a914') + script_hash + bytes.fromhex('87')
scripthash = sha256(scriptPubKey).digest()[::-1].hex()
```

### What to build

**`p2shScripthash(p2shAddress)`** — compute ElectrumX scripthash for a P2SH address.

**`subscribeToP2SHScripts()`** — iterate `self.scripts`, subscribe to each funded/active script. Called by the application after ElectrumX is connected (same timing as `subscribeToScripthashActivity`).

**`_handleP2SHNotification(p2shAddress, notification)`** — when a P2SH scripthash status changes, query unspents to check if the funding UTXO still exists. Update script status. Fire optional callback.

**Modify `saveScript()`** — when a script transitions to `funded`, subscribe to it if ElectrumX is connected.

**New callback** — `scriptStatusCallback(p2shAddress, oldStatus, newStatus)` — so the application can react to state changes. Follows existing `balanceUpdatedCallback` pattern.
