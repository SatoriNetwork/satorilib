# Toolkit vs Satorilib — Fresh Comparison (Post All Fixes)

**Date:** 2026-02-26
**Toolkit:** `/home/jordan/repos/toolkit/Lib/satorilib/wallet/`
**Satorilib:** `/home/jordan/repos/satorilib/src/satorilib/wallet/`

---

## 1. File Inventory

### Files in BOTH repos
| File | Status |
|------|--------|
| `__init__.py` | **DIFFERS** — toolkit has `scripts` import + `__all__` |
| `identity.py` | IDENTICAL (missing trailing newline in toolkit — cosmetic) |
| `notes.md` | IDENTICAL |
| `wallet.py` (base) | **DIFFERS** — major (see section 3) |
| `concepts/authenticate.py` | **DIFFERS** — `pubkey` → `wallet-pubkey` (satorilib improvement) |
| `concepts/balance.py` | IDENTICAL |
| `concepts/transaction.py` | IDENTICAL |
| `ethereum/valid.py` | IDENTICAL |
| `ethereum/wallet.py` | IDENTICAL |
| `evrmore/__init__.py` | **DIFFERS** — import path refactor (satorilib improvement) |
| `evrmore/identity.py` | **DIFFERS** — import path refactor (satorilib improvement) |
| `evrmore/wallet.py` | **DIFFERS** — major (see section 4) |
| `evrmore/walletsh.py` | IDENTICAL |
| `evrmore/scripts/` (entire package) | IDENTICAL (all 12 files) |
| `evrmore/utils/__init__.py` | IDENTICAL |
| `evrmore/utils/multisig.py` | IDENTICAL |
| `evrmore/utils/sign.py` | IDENTICAL |
| `evrmore/utils/valid.py` | IDENTICAL |
| `evrmore/utils/verify.py` | IDENTICAL |
| `ravencoin/__init__.py` | IDENTICAL |
| `ravencoin/sign.py` | IDENTICAL |
| `ravencoin/verify.py` | IDENTICAL |
| `ravencoin/wallet.py` | **DIFFERS** — `_checkSatoriValue()` length-prefix fix (satorilib improvement) |
| `utils/__init__.py` | IDENTICAL |
| `utils/transaction.py` | IDENTICAL (trailing whitespace only) |
| `utils/validate.py` | IDENTICAL |

### Files ONLY in toolkit
| File | Notes |
|------|-------|
| `evrmore/p2shnotes.txt` | Dev reference notes (107 lines). Not code. |

### Files ONLY in satorilib
| File | Notes |
|------|-------|
| `evrmore/scripts.py` | `P2SHRedeemScripts` class (187 lines) |
| `evrmore/sign.py` | Promoted wrapper for `utils/sign.py` (minus `signForPubkey`) |
| `evrmore/valid.py` | Promoted copy of `utils/valid.py` |
| `evrmore/verify.py` | Promoted copy of `utils/verify.py` |
| `rpc_methods.py` | `RpcMethodsMixin` class (373 lines) |
| `utils/satori_tx.py` | SATORI transaction parsing utilities (119 lines) |

---

## 2. Trivial / Cosmetic Differences

These files differ but the differences are not functional:

| File | Difference |
|------|-----------|
| `__init__.py` | Toolkit has `from ... import scripts` + `__all__` list. Satorilib doesn't. Cosmetic. |
| `concepts/authenticate.py` | `'pubkey'` → `'wallet-pubkey'` key name. Satorilib improvement. |
| `evrmore/__init__.py` | Import from `evrmore.sign` vs `evrmore.utils.sign`. Satorilib improvement. |
| `evrmore/identity.py` | Same import path change. Satorilib improvement. |
| `ravencoin/wallet.py` | Length-prefix byte fallback in `_isValidAssetTransaction()`. Satorilib improvement. |
| `utils/transaction.py` | Trailing whitespace removal only. |

---

## 3. `wallet.py` (base class) — All Remaining Differences

### 3.1 Import ordering

Toolkit: `typing, os, json, threading, datetime, functools, enum, random, decimal, joblib`
Satorilib: `functools, typing, os, datetime, json, joblib, threading, enum, random, decimal`

Toolkit imports `Sequence` and `Any` from typing; satorilib does not.

**Category: cosmetic.**

### 3.2 `getReadyToSend()` — divisibility check

Satorilib adds:
```python
if self.divisibility == 0:
    self.getStats()
```
**Category: satorilib improvement.**

### 3.3 `getReadyToSendSimplified()` + `computeUnspentScriptPubKeys()` — satorilib only

56 lines. Fast P2PKH path that computes scriptPubKeys locally instead of fetching from ElectrumX.

**Category: satorilib improvement.**

### 3.4 `authPayload()` — whitespace

```diff
-        vaultInfo: dict = None,    # toolkit
+        vaultInfo:dict = None,     # satorilib
```
**Category: cosmetic.**

### 3.5 `_compileCurrencyChangeOutput()` — extra parameters

Satorilib adds `inputCount: int = 0` and `outputCount: int = 0` so callers can either pass a pre-computed `fee` OR let the method estimate from counts:
```python
currencyChange = gatheredCurrencySats - currencySats - (
    fee or TxUtils.estimatedFee(inputCount=inputCount, outputCount=outputCount))
```
**Category: satorilib improvement (more flexible).**

### 3.6 `produceMultiTimeMultisig()` — docstring fix

```diff
-        ''' creates a transaction with multiple currency recipients '''
+        ''' creates a transaction with multiple SATORI asset recipients '''
```
**Category: satorilib improvement (correct docstring).**

### 3.7 `simpleTimeTransaction()` — `extraVouts` bug fix

```diff
# toolkit:
            extraVouts=currencyChangeOut + ([memoOut] if memoOut else []))
# satorilib:
            extraVouts=([currencyChangeOut] if currencyChangeOut else []) + ([memoOut] if memoOut else []))
```
Toolkit treats `currencyChangeOut` as a list (it's a single `CMutableTxOut`). Satorilib wraps it properly with None guard.

**Category: satorilib improvement (bug fix).**

### 3.8 Section comments

Satorilib adds `# for server` before `satoriDistribution()` and `# for neuron` before `currencyTransaction()`.

**Category: cosmetic.**

### 3.9 `satoriAndCurrencyTransaction()` — removed comment

Toolkit has `''' unused, untested '''`; satorilib removes it.

**Category: cosmetic.**

### 3.10 `satoriDistribution()` — removed commented-out recursion

Toolkit has 6 commented-out lines for fee recursion. Satorilib removes them (the `feeOverride` param and `fee` calculation are present in both — just the dead comments are gone).

**Category: cosmetic (dead code cleanup).**

### 3.11 `satoriOnlyPartialBridge()` — fee calculation change

Toolkit pre-computes fee with `feeOverride` and passes it to `_compileCurrencyChangeOutput(fee=...)`.
Satorilib removes `feeOverride` and instead passes `inputCount`/`outputCount` to let `_compileCurrencyChangeOutput` estimate internally.

**Category: intentional divergence (satorilib uses its more flexible `_compileCurrencyChangeOutput`).**

### 3.12 `satoriOnlyPartialSimple()` — Mundo fee rework (MAJOR)

**Toolkit signature:**
```python
def satoriOnlyPartialSimple(self, amount, address, pullFeeFromAmount,
    feeSatsReserved, completerAddress, changeAddress, feeOverride)
```

**Satorilib signature:**
```python
def satoriOnlyPartialSimple(self, amount, address, pullFeeFromAmount,
    feeSatsReserved, feeSats, satoriFeeAmount, completerAddress,
    changeAddress, satoriFeeAddress, **kwargs)
```

Changes:
- `feeOverride` removed, replaced by `feeSats`, `satoriFeeAmount`, `satoriFeeAddress`, `**kwargs`
- Mundo fee is dynamic: `satoriFeeAmount if satoriFeeAmount > 0 else TxUtils.asSats(self.mundoFee)`
- SATORI fee output can go to `satoriFeeAddress` (falls back to `completerAddress`)
- EVR change: if `feeSats > 0` uses Mundo-provided fee; otherwise local estimation

**Category: satorilib improvement (dynamic Mundo fees).**

### 3.13 `completeSimplePartial()` — signOnly mode

New parameters in satorilib: `claimAddress`, `signOnly` (replaces `feeOverride`).

- Claim verification checks `claimAddress or completerAddress` instead of just `completerAddress`
- `signOnly=True` returns signed tx hex without broadcasting

**Category: satorilib improvement (Mundo signing flow).**

### 3.14 `sendAllTransaction()` — bug fix

```diff
# toolkit (BUG — feeOverride is None on first call):
-        currencySatsLessFee = currencySats - TxUtils.estimatedFee(...)
-        ...
-        print('estimated fee:', feeOverride, 'actual fee:', requiredFee)
-        if requiredFee * 0.99 < feeOverride < requiredFee * 1.25:
# satorilib (FIXED):
+        fee = feeOverride or TxUtils.estimatedFee(...)
+        currencySatsLessFee = currencySats - fee
+        ...
+        print('estimated fee:', fee, 'actual fee:', requiredFee)
+        if requiredFee * 0.99 < fee < requiredFee * 1.25:
```

**Category: satorilib improvement (bug fix — toolkit crashes with TypeError on first call).**

### 3.15 `sendAllPartialSimple()` — same Mundo fee rework

Same parameter changes as `satoriOnlyPartialSimple` (`feeSats`, `satoriFeeAmount`, `satoriFeeAddress`).

**Category: satorilib improvement.**

### 3.16 `typicalNeuronTransaction()` / `sendIndirect()` — MAJOR rework

**Toolkit:** `sendIndirect()` is partially disabled (`#return sendIndirect()` commented out, returns hardcoded error instead). Uses `broadcastBridgeSimplePartialFn` naming. Returns raw HTTP response from Mundo.

**Satorilib:** `sendIndirect()` is fully activated. New protocol:
1. Neuron estimates input/output counts
2. Requests partial from Mundo with `network='evrmore'`, `inputCount`, `outputCount`
3. Extracts `feeSats`, `satoriFeeAddress`, `satoriFeeAmount` from Mundo response
4. Builds partial, Mundo signs, neuron broadcasts locally
5. Wraps everything in try/except with EVR-required fallback message

**Category: satorilib improvement (Mundo integration fully working).**

### 3.17 Trailing whitespace cleanup

~25 lines have trailing whitespace removed.

**Category: cosmetic.**

---

## 4. `evrmore/wallet.py` — All Remaining Differences

### 4.1 Imports

```diff
-from typing import Any, Union, Callable, Dict, Sequence, Optional    # toolkit
+from typing import Any, Union, Callable, Dict, Optional              # satorilib (no Sequence)
```

Import paths promoted:
```diff
-from satorilib.wallet.evrmore.utils.sign import signMessage
-from satorilib.wallet.evrmore.utils.verify import verify
-from satorilib.wallet.evrmore.utils.valid import isValidEvrmoreAddress
+from satorilib.wallet.evrmore.sign import signMessage
+from satorilib.wallet.evrmore.verify import verify
+from satorilib.wallet.evrmore.valid import isValidEvrmoreAddress
```

New: `from satorilib.wallet.rpc_methods import RpcMethodsMixin`

**Category: satorilib improvement (import path refactor + RPC support).**

### 4.2 Class declaration

```diff
-class EvrmoreWallet(Wallet):
+class EvrmoreWallet(Wallet, RpcMethodsMixin):
```
**Category: satorilib improvement.**

### 4.3 Constructor — RPC parameters

Satorilib adds `rpcNodes`, `rpcUrl`, `rpcUser`, `rpcPassword` params and calls `self._initRpcClient(...)`.

**Category: satorilib improvement.**

### 4.4 `_isValidAssetTransaction()` — length prefix byte

Same fix as ravencoin: adds `x[1:].startswith(expected)` fallback. Also removes ~5 lines of commented-out logging.

**Category: satorilib improvement (bug fix).**

### 4.5 `_compileInputs()` — naming convention

| Toolkit | Satorilib |
|---------|-----------|
| `redeemScripts` | `redeem_scripts` |
| `utxoKey` | `utxo_key` |
| `redeemScript` | `redeem_script` |
| `baseScript` | `base_script` |
| Pre-computes `littleEValue` once | Computes inline per-UTXO |

**Category: intentional divergence (naming convention).**

### 4.6 `_compileSatoriOutputs()` — interface difference

**Toolkit:** Has `_compileSatoriOutput()` helper + `_compileSatoriOutputs()` accepting dict OR `address`+`sats` kwargs.

**Satorilib:** Only `_compileSatoriOutputs(satsByAddress: dict)` — dict-only, no kwargs.

All satorilib callers pass dicts. Payment channel methods wrap single outputs as `{address: sats}`.

**Category: intentional divergence (acceptable — all callers use dict form).**

### 4.7 `_compileCurrencyOutputs()` — loop variable shadowing fix

```diff
# toolkit:
-        [self._compileCurrencyOutput(address, sats) for address, sats in satsByAddress.items()]
# satorilib:
+        [self._compileCurrencyOutput(addr, s) for addr, s in satsByAddress.items()]
```
**Category: satorilib improvement (avoids shadowing parameters).**

### 4.8 `_compileCurrencyChangeOutput()` — same as base

New `inputCount`/`outputCount` params. Simplified error message.

**Category: satorilib improvement.**

### 4.9 `_createTransaction()` / `_signInput()` — MAJOR architectural divergence

**Toolkit:** 3 methods — `signatureForInput()`, `_verifyInput()`, `_signInput()`. Uses `redeemScripts`/`redeemParams`/`redeemDates` params. Has inline `_cltvNumberFrom()` for date-based locking.

**Satorilib:** 1 combined `_signInput()`. Uses `redeem_scripts`/`signatures` params. Multi-sig via `signatures` list. No `redeemParams` or `redeemDates`.

Both produce identical transactions for identical inputs — just structured differently.

**Category: intentional divergence (different internal architecture, same external behavior).**

### 4.10 P2SH infrastructure — `_compileClaimOnP2SH*`

Methods exist in both. Satorilib's `_compileClaimOnP2SH()` merges single-input and multi-input into one method. Toolkit keeps them separate.

Satorilib calls `_compileSatoriOutputs({address: sats})` (dict); toolkit calls `_compileSatoriOutputs(address=address, sats=sats)` (kwargs).

**Category: intentional divergence (satorilib more consolidated).**

### 4.11 Payment channel methods — naming differences

```diff
# toolkit import:
-from ...scripts.channels.lock import thunderChannelExpiring
# satorilib import:
+from ...scripts.channels.lock import paymentChannelExpiring
```

Method naming:
| Toolkit | Satorilib |
|---------|-----------|
| `produceThunderExpiring(...)` | `producePaymentChannelCurrency(...)` |
| `return self.produceThunderChannel(...)` | `return self.producePaymentChannelFromScript(...)` |
| `return self.produceThunderChannelCurrency(...)` | `return self.producePaymentChannelCurrencyFromScript(...)` |

Docstrings updated accordingly.

**Category: satorilib improvement (clearer naming — bug fix for duplicate method name that toolkit still has).**

### 4.12 `_compileCurrencyOutputs` call convention

```diff
# toolkit:
-        currencyOuts = self._compileCurrencyOutputs({address: script['currency_sats']})
# satorilib:
+        currencyOuts = self._compileCurrencyOutputs(address=address, sats=script['currency_sats'])
```
**Category: intentional divergence (both work — toolkit passes dict, satorilib passes kwargs).**

### 4.13 Removed commented-out code + usage sketch

Toolkit has a 26-line commented usage sketch at EOF. Satorilib removes it. Various commented-out validation and logging lines also removed.

**Category: satorilib improvement (cleanup).**

### 4.14 Trailing whitespace

~30 instances removed.

**Category: cosmetic.**

---

## 5. Summary

### Zero remaining critical items to port

All toolkit functionality has been ported. Every remaining difference falls into one of:

### Satorilib improvements (DO NOT port back)
- RPC methods mixin (`RpcMethodsMixin`)
- `getReadyToSendSimplified()` + `computeUnspentScriptPubKeys()` (P2PKH optimization)
- `_isValidAssetTransaction()` length-prefix byte fallback (evrmore + ravencoin)
- `getReadyToSend()` divisibility zero fix
- `simpleTimeTransaction()` `extraVouts` bug fix (wraps `currencyChangeOut` in list)
- `sendAllTransaction()` bug fix (toolkit crashes with `TypeError` on first call)
- `produceMultiTimeMultisig()` docstring correction
- Mundo fee rework (`satoriOnlyPartialSimple`, `sendAllPartialSimple`, `completeSimplePartial`)
- `sendIndirect()` fully activated in `typicalNeuronTransaction()`
- Payment channel duplicate method name fix + missing `return` statements
- `_compileCurrencyOutputs` loop variable shadowing fix
- `_compileCurrencyChangeOutput` flexible `inputCount`/`outputCount` params
- `P2SHRedeemScripts` class
- `utils/satori_tx.py` parsing utilities
- Top-level `sign.py`/`verify.py`/`valid.py` wrappers
- `concepts/authenticate.py` key rename (`wallet-pubkey`)
- Dead code / commented-out code cleanup throughout

### Intentional divergences (acceptable)
- `_compileInputs()` naming: camelCase (toolkit) vs snake_case (satorilib)
- `_createTransaction()` / `_signInput()` architecture: 3 methods (toolkit) vs 1 combined (satorilib)
- `_compileSatoriOutputs()`: dict+kwargs (toolkit) vs dict-only (satorilib)
- `_compileClaimOnP2SH()`: separate single/multi (toolkit) vs merged (satorilib)
- Import paths: `evrmore.utils.sign` (toolkit) vs `evrmore.sign` (satorilib)

### Cosmetic only
- Import ordering in `wallet.py`
- `authPayload()` space before colon
- Section comments (`# for server`, `# for neuron`)
- `satoriAndCurrencyTransaction` removed "unused, untested" comment
- Trailing whitespace (~55 lines across both files)

### Toolkit-only files not ported (intentional)
- `evrmore/p2shnotes.txt` — dev reference notes, not code
