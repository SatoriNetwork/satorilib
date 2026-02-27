# Toolkit vs Satorilib — Post-Merge Comparison

**Date:** 2026-02-26 (updated after all fixes applied)
**Toolkit (source):** `/home/jordan/repos/toolkit/Lib/satorilib/wallet/`
**Satorilib (target):** `/home/jordan/repos/satorilib/src/satorilib/wallet/`

---

## 1. File Inventory

### Files in BOTH repos
| File | Status |
|------|--------|
| `evrmore/__init__.py` | **DIFFERS** — import path refactor (satorilib improvement) |
| `evrmore/wallet.py` | **DIFFERS** — see section 3 (intentional divergences only) |
| `evrmore/identity.py` | **DIFFERS** — import paths only (satorilib improvement) |
| `evrmore/walletsh.py` | IDENTICAL |
| `evrmore/utils/__init__.py` | IDENTICAL (empty) |
| `evrmore/utils/sign.py` | IDENTICAL |
| `evrmore/utils/verify.py` | IDENTICAL |
| `evrmore/utils/valid.py` | IDENTICAL |
| `evrmore/utils/multisig.py` | IDENTICAL |
| `evrmore/scripts/` (entire package) | IDENTICAL (all subpackages) |
| `wallet.py` (base) | **DIFFERS** — see section 4 (intentional divergences only) |
| `identity.py` | IDENTICAL |
| `utils/validate.py` | IDENTICAL |
| `utils/transaction.py` | IDENTICAL (fee utilities ported) |
| `concepts/authenticate.py` | **DIFFERS** — `pubkey` → `wallet-pubkey` key name (satorilib improvement) |
| `concepts/balance.py` | IDENTICAL |
| `concepts/transaction.py` | IDENTICAL |
| `ravencoin/wallet.py` | **DIFFERS** — `_checkSatoriValue()` fix (satorilib improvement) |
| `ethereum/wallet.py` | IDENTICAL |
| `ethereum/valid.py` | IDENTICAL |
| `__init__.py` (wallet pkg) | **DIFFERS** — toolkit has `__all__`; cosmetic |

### Files ONLY in satorilib (not in toolkit)
| File | Description | Category |
|------|------------|----------|
| `evrmore/sign.py` | Top-level sign module (delegates to utils/) | satorilib improvement |
| `evrmore/verify.py` | Top-level verify module (delegates to utils/) | satorilib improvement |
| `evrmore/valid.py` | Top-level valid module (delegates to utils/) | satorilib improvement |
| `evrmore/scripts.py` | `P2SHRedeemScripts` class (higher-level script creation) | satorilib improvement |
| `rpc_methods.py` | `RpcMethodsMixin` for direct RPC node access | satorilib improvement |
| `utils/satori_tx.py` | SATORI transaction parsing utilities | satorilib improvement |

### Files ONLY in toolkit — NONE

### Cleanup completed
- ~~`evrmore/from satorilib.py`~~ — Junk REPL scratch file — **DELETED**

---

## 2. `evrmore/__init__.py` & `evrmore/identity.py`

**Category: satorilib improvement (import path refactor)**

Satorilib has top-level `sign.py`, `verify.py`, `valid.py` wrapper modules in `evrmore/` that
delegate to `utils/`. Imports go through these wrappers for cleaner paths:
- Toolkit: `from satorilib.wallet.evrmore.utils.sign import signMessage`
- Satorilib: `from satorilib.wallet.evrmore.sign import signMessage`

Functionally identical. `identity.py` has the same difference.

---

## 3. `evrmore/wallet.py` — Remaining Differences

### 3.1 Class Declaration & Imports

| Aspect | Toolkit | Satorilib | Category |
|--------|---------|-----------|----------|
| RPC mixin | None | `RpcMethodsMixin` in bases | satorilib improvement |
| Import paths | `from ...utils.sign/verify/valid` | `from ...sign/verify/valid` | satorilib improvement |
| `Sequence` import | Yes | No | satorilib cleanup |

### 3.2 `create()` + `__init__()`

Satorilib adds RPC params (`rpcNodes`, `rpcUrl`, `rpcUser`, `rpcPassword`) and calls
`self._initRpcClient(...)`. **Category: satorilib improvement.**

### 3.3 `_checkSatoriValue()` — Bug Fix

Satorilib adds a fallback that strips the length prefix byte, handling a real-world format
variation in SATORI asset scriptPubKeys. **Category: satorilib improvement (bug fix).**

### 3.4 `_compileInputs()`

| Aspect | Toolkit | Satorilib |
|--------|---------|-----------|
| Param names | `redeemScripts` (camelCase) | `redeem_scripts` (snake_case) |
| Variable names | `utxoKey`, `redeemScript` | `utxo_key`, `redeem_script` |
| Asset value computation | Pre-computes `littleEValue` before loop | Computes inline per-UTXO |

**Category: intentional divergence (naming convention).**

### 3.5 P2SH Infrastructure — `_compileClaimOnP2SH*`

Both have the same methods. Satorilib's `_compileClaimOnP2SH()` supports BOTH single
(`fundingTxId`) and multi-input (`fundingTxIds`) forms in one method; toolkit keeps them
separate. **Category: intentional divergence.** Satorilib's is more consolidated.

### 3.6 `_compileCurrencyOutput()` / `_compileCurrencyOutputs()`

Both have the singular helper and flexible plural method. **IDENTICAL after port.**

### 3.7 `_createTransaction()` / `_signInput()` / `_verifyInput()`

| Aspect | Toolkit | Satorilib |
|--------|---------|-----------|
| Signing | Separate `signatureForInput()` + `_signInput()` + `_verifyInput()` | Combined `_signInput()` does everything |
| UTXO key format | `b2lx(txin.prevout.hash)` (little-endian) | `b2lx(txin.prevout.hash)` (little-endian) — **FIXED** |
| P2SH in _createTransaction | Handles `redeemScripts`, `redeemParams`, `redeemDates` dicts | Handles `redeem_scripts` and `signatures` dicts |

The `b2x` vs `b2lx` issue has been **RESOLVED** — satorilib now uses `b2lx` matching toolkit.
The P2SH dict path in `_createTransaction()` is dead code in practice (P2SH goes through
`_compileClaimOnP2SH*` which handles signing internally), so the naming convention difference
is cosmetic. **Category: intentional divergence (acceptable).**

### 3.8 `_compileSatoriOutputs()`

Toolkit has flexible signature (dict or `address`+`sats`). Satorilib only has dict form.
All callers pass dicts. **Category: intentional divergence (acceptable).**

### 3.9 Payment Channel Methods

All 16 payment channel methods are present in both. **Category: IDENTICAL after port.** Satorilib has
bug fixes (missing `return` statements, duplicate method name) that toolkit still lacks.

---

## 4. `wallet.py` (base class) — Remaining Differences

### 4.1 P2SH Lock Methods (`produce*`)

All 4 methods present in both with recursive fee correction. **IDENTICAL after port.**

### 4.2 P2SH Unlock Methods (`simpleTime*`, `multiTime*`)

All 12 methods present in both. **IDENTICAL after port.**

### 4.3 `_gatherCurrencyUnspents()` — `feeOverride` Support

Both now have `feeOverride` and `feeRate` params. **IDENTICAL after port.**

### 4.4 `_compileCurrencyChangeOutput()` — `fee` Param

Satorilib keeps BOTH `inputCount`/`outputCount` AND `fee` params. Toolkit only has `fee`.
Existing satorilib callers use `inputCount`/`outputCount`; new p2sh callers use `fee`.
**Category: satorilib improvement (more flexible).**

### 4.5 `getReadyToSend()` — Divisibility Fix

Satorilib adds divisibility zero check. **Category: satorilib improvement.**

### 4.6 `getReadyToSendSimplified()` + `computeUnspentScriptPubKeys()` — satorilib only

Major P2PKH performance optimization. **Category: satorilib improvement.**

### 4.7 P2PKH Transaction Methods — Fee Correction

All 5 methods now have recursive fee correction matching toolkit:

| Method | Status |
|--------|--------|
| `satoriDistribution` | `feeOverride` param added, fee passed to change calc. **No recursion** (matching toolkit where recursion is commented out — see note below) |
| `currencyTransaction` | Full recursive fee correction + `broadcast` toggle. **MATCHES toolkit.** |
| `satoriTransaction` | Full recursive fee correction. **MATCHES toolkit.** |
| `satoriAndCurrencyTransaction` | Full recursive fee correction. **MATCHES toolkit.** |
| `sendAllTransaction` | Full recursive fee correction. **IMPROVED over toolkit** — fixed a bug where toolkit compares `feeOverride` directly (which is `None` on first call, causing `TypeError`). Satorilib uses a local `fee` variable instead. |

**Note on `satoriDistribution`:** Toolkit has the recursion commented out for this method.
This is intentional — `satoriDistribution` can have up to 1000 recipients, making it a large
transaction where the per-vin/per-vout fee estimate is already accurate enough as a percentage
of total fee. The cost of rebuilding a 1000-output transaction for marginal fee improvement
isn't worth it.

### 4.8 Mundo Integration Methods

Satorilib has significant improvements. **Category: satorilib improvement.**

---

## 5. Supporting Files

### 5.1 `utils/transaction.py` — Fee Utilities

**RESOLVED** — satorilib now has all toolkit's fee utilities:
- `feeRatePerVin`, `feeRatePerVout`, `feeRate`, `defaultFee` class attributes
- `getTxSize()` and `getTxFee()` static methods

### 5.2 `concepts/authenticate.py`

Single key rename: `pubkey` → `wallet-pubkey`. **Category: satorilib improvement.**

---

## 6. Summary — FINAL STATUS

### All Critical Items: RESOLVED

1. ~~**`TxUtils` fee utilities**~~ — **PORTED.** `feeRate`, `defaultFee`, `getTxSize()`, `getTxFee()` added.
2. ~~**Recursive fee correction**~~ — **PORTED** to `currencyTransaction`, `satoriTransaction`,
   `satoriAndCurrencyTransaction`, `sendAllTransaction`. Fixed toolkit bug in `sendAllTransaction`.
3. ~~**Junk file**~~ — **DELETED** (`evrmore/from satorilib.py`).
4. ~~**`b2x` vs `b2lx`**~~ — **FIXED.** Changed to `b2lx` in `_createTransaction()`.

### Satorilib Improvements (DO NOT port back to toolkit)

- RPC methods mixin
- `getReadyToSendSimplified()` performance optimization
- `_checkSatoriValue()` length-prefix fallback
- Mundo integration improvements (feeSats, signOnly, self-broadcasting)
- `getReadyToSend()` divisibility fix
- Payment channel `return` statement fixes
- Payment channel duplicate method name fix
- `P2SHRedeemScripts` class
- `satori_tx.py` parsing utilities
- Top-level sign/verify/valid wrappers
- `_compileCurrencyChangeOutput` keeping both `fee` and `inputCount`/`outputCount`
- `sendAllTransaction` bug fix (toolkit's `None` comparison)

### Intentional Divergences (acceptable)

- P2SH infrastructure on base `Wallet` vs `EvrmoreWallet`
- Combined vs separated `_signInput()` architecture
- camelCase vs snake_case in some variables
- `_compileSatoriOutputs()` dict-only vs flexible signature
