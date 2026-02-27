# Evrmore Wallet Merge Scratchpad

## Goal
Make `/home/jordan/repos/satorilib/` (the active repo) the most up-to-date version,
incorporating good code from the other copies.

## Repo Locations
| Label | Path | Notes |
|-------|------|-------|
| **satorilib** (active) | `/home/jordan/repos/satorilib/src/satorilib/wallet/evrmore/` | New consolidated repo, currently in production |
| **Satori/Lib** (main) | `/home/jordan/repos/Satori/Lib/satorilib/wallet/evrmore/` | Old production repo, `main` branch |
| **Satori/Lib** (ai-p2sh) | Same path, branch `ai-p2sh` | P2SH/thunder development branch |
| **Satori/Lib** (stash) | Same path, `stash@{0}` on ai-p2sh | WIP thunder channel renaming + multi-input support |
| **toolkit** | `/home/jordan/repos/toolkit/Lib/satorilib/wallet/evrmore/` | Most developed thunder channel code (59KB wallet.py) |

---

## Step 1: Compare Satori/Lib (main) vs satorilib (active) — EVRMORE WALLET FILES

### File Inventory

| File | Satori/Lib (main) | satorilib (active) | Status |
|------|-------------------|-------------------|--------|
| `wallet.py` | 38,064 bytes | 38,438 bytes | **DIFFERS** — satorilib is newer |
| `walletsh.py` | 10,836 bytes | 10,836 bytes | IDENTICAL |
| `scripts.py` | 7,836 bytes | 7,836 bytes | IDENTICAL |
| `valid.py` | 2,085 bytes | 2,085 bytes | IDENTICAL |
| `sign.py` | 596 bytes | 624 bytes | **DIFFERS** |
| `verify.py` | 1,049 bytes | 1,312 bytes | **DIFFERS** |
| `__init__.py` | 228 bytes | 228 bytes | IDENTICAL |
| `from satorilib.py` | 272 bytes | 272 bytes | IDENTICAL |
| `identity.py` | NOT PRESENT | 2,572 bytes (76 lines) | **satorilib only** |
| `p2shnotes.txt` | 3,656 bytes | NOT PRESENT | **Satori/Lib only** (dev notes) |
| `scripts/` (directory) | PRESENT (channels, mining, multisig, p2pkh) | NOT PRESENT | **Satori/Lib only** |
| `utils/` (directory) | PRESENT (multisig.py, sign.py, valid.py, verify.py) | NOT PRESENT | **Satori/Lib only** |

> Note: `scripts/` and `utils/` only exist on `main` as compiled `.pyc` files. The source `.py` files exist on the `ai-p2sh` branch.

---

### Detailed Diffs: Files that differ between Satori/Lib (main) → satorilib (active)

#### 1. `wallet.py` — The big one

**satorilib has these improvements over Satori/Lib main:**

- **New imports**: `Dict` typing, `Identity`, `EvrmoreIdentity`, `RpcMethodsMixin`
- **Class inherits `RpcMethodsMixin`**: `class EvrmoreWallet(Wallet, RpcMethodsMixin)`
- **Constructor refactored**:
  - Removed: `isTestnet`, `useElectrumx`, `kind` params
  - Added: `cachePath`, `identity`, `rpcNodes`, `rpcUrl`, `rpcUser`, `rpcPassword` params
  - `walletPath` default changed from hardcoded path to `None`
  - Now creates `EvrmoreIdentity` and `Electrumx` objects in outer `create()` and passes them in
  - RPC client initialization added via `_initRpcClient()`
- **Identity pattern**: All references to `self._privateKeyObj` changed to `self.identity._privateKeyObj`
  and `self._addressObj` to `self.identity._addressObj` (lines 163, 198, 371, 478, 494-495)
- **New `generateOtp()` method** (lines 171-173)
- **`maybeConnect()` simplified**: Removed `useElectrumx` guard, flattened nesting
- **`_validateMundoVout()` improved**: New logic handles both formats (with/without asset protocol length prefix byte)
  ```python
  # OLD: simple startswith check, returned False on failure
  # NEW: checks both x.startswith(expected) and x[1:].startswith(expected)
  ```
- **Trailing whitespace/newline cleanup**

#### 2. `sign.py`

```python
# Satori/Lib (main):
from evrmore.signmessage import EvrmoreMessage, SignMessage
...
return SignMessage(...)

# satorilib (active):
from evrmore.signmessage import signMessage as sm
from evrmore.signmessage import EvrmoreMessage
...
return sm(...)
```
Rename from `SignMessage` to `signMessage as sm` — likely matching an API change in the evrmore library.

#### 3. `verify.py`

satorilib version is larger (1,312 vs 1,049 bytes). Exact diff not captured but likely has additional verification logic.

#### 4. `identity.py` (satorilib only)

76-line `EvrmoreIdentity` class that extends a base `Identity` class. Encapsulates:
- Private key management
- Address generation
- Chain selection (`SelectParams`)
- Message signing/verification
- OTP generation

This is part of the architectural refactor where identity concerns were extracted out of the wallet.

---

### Summary: satorilib is AHEAD of Satori/Lib main in these areas:
1. Identity pattern extraction (cleaner separation of concerns)
2. RPC support (rpcNodes, rpcUrl, rpcUser, rpcPassword)
3. Constructor simplification (removed unused params like isTestnet, useElectrumx, kind)
4. Evrmore library API updates (sign.py import changes)
5. Mundo vout validation fix (handles both asset protocol formats)
6. OTP generation

### Summary: Satori/Lib main has things satorilib does NOT:
1. `p2shnotes.txt` — development notes (not critical, just reference)
2. `scripts/` directory — but only as `.pyc` files on main (source is on `ai-p2sh` branch)
3. `utils/` directory — same, only `.pyc` on main

---

## Step 1.5: Satori/Lib `ai-p2sh` branch additions (over Satori/Lib main)

The `ai-p2sh` branch is a **massive** change: **+10,151 / -1,126 lines across 84 files**.
Most of it is outside the evrmore wallet (centrifugo, datamanager, sqlite, etc.).

**Evrmore wallet-specific changes on ai-p2sh:**
- `__init__.py` — Restructured imports to use `utils/` subpackage, added `scripts` import, added `__all__`
- `identity.py` — **NEW** 76-line EvrmoreIdentity class (same as what's in satorilib now)
- `scripts.py` — **DELETED** (replaced by `scripts/` package)
- `scripts/` package — **NEW** with submodules:
  - `channels/lock.py` (171 lines) — `renewableThunderChannel`, `nonrenewableThunderChannel`
  - `channels/unlock.py` (30 lines)
  - `mining/lock.py` (412 lines) — `simpleTime`, `multiTime`, `multiTimeMultisig`
  - `mining/unlock.py` (68 lines)
  - `multisig/lock.py` (29 lines) — `basicMultisig`
  - `multisig/unlock.py` (47 lines)
  - `p2pkh/unlock.py` (10 lines)
- `sign.py` — **DELETED** (moved to `utils/sign.py`)
- `utils/` package — **NEW**:
  - `sign.py` (29 lines)
  - `valid.py` (moved from evrmore/)
  - `verify.py` (moved + 16 lines changed)
  - `multisig.py` (250 lines) — `MultisigUtils` class
- `wallet.py` — **490 lines changed** — Major refactor: identity pattern, p2sh support, claim methods
- `from satorilib.py` — **DELETED** (moved to `tests/manual/`)

**Also added outside evrmore/:**
- `wallet/identity.py` (747 lines) — Base Identity class
- `wallet/utils/transaction.py` (20 lines)
- `wallet/wallet.py` — **1,436 lines changed** (base Wallet class major refactor)
- `tests/p2sh/` — Test files for p2sh (multisig, timerelease, asset, currency)
- `tests/test_identity.py` (264 lines)

---

## Step 1.6: Stash on ai-p2sh — `stash@{0}: WIP on ai-p2sh: 807f350 thunder scripts`

4 files changed, 772 insertions, 87 deletions:

1. **`scripts/__init__.py`** — Renames:
   - `renewableThunderChannel` → `thunderChannel`
   - `nonrenewableThunderChannel` → `thunderExpiring`

2. **`utils/multisig.py`** — Minor:
   - `dict[dt.datetime, dict]` → `dict[Any, dict]` (type annotation fix)
   - Added `### CREATE ###` section header
   - Import `Any` added

3. **`evrmore/wallet.py`** — Multi-input P2SH support:
   - `_compileClaimOnP2SHMultiSigStart()`: Changed from single `fundingTxId`/`fundingVout`/`date`
     to lists: `fundingTxIds`/`fundingVouts`/`dates` — supports multiple P2SH inputs in one tx
   - `_compileClaimOnP2SHMultiSigMiddle()`: Added `redeemCount` param, loop over multiple redeem inputs
   - Parameter renames: `address` → `toAddress`

4. **`wallet/wallet.py`** — **MASSIVE** +798 lines. Major additions to base Wallet class:
   - Thunder channel management methods (create, claim, close)
   - Multi-signature coordination
   - P2SH transaction building and signing
   - Channel state tracking
   - (This is the bulk of the thunder/spillman protocol implementation)

---

## Progress Tracker

- [x] Step 1: Compare Satori/Lib (main) vs satorilib — evrmore wallet files
- [x] Step 2: Compare Satori/Lib (ai-p2sh + stash) vs satorilib — covered in sections 1.5 & 1.6
- [x] Step 3: Compare toolkit vs Satori/Lib (ai-p2sh + stash) — find toolkit-only improvements
- [x] Step 4: Plan what to port into satorilib
- [ ] Step 5: Begin porting changes

---

## Step 3: Toolkit vs Satori/Lib ai-p2sh (+ stash) — THE LINEAGE

### Timeline & Lineage (this answers the key question)

```
ai-p2sh branch development:
  Jul 2-Aug 23, 2025    Various commits (centrifugo, otp, scripts, thunder signing)
  Aug 23 (807f350)       "thunder scripts" — LAST COMMIT on ai-p2sh
  [stash created]        WIP: multi-input, thunderChannel rename

toolkit fork:
  Aug 29 (2ddba25)       "fork" — forked FROM ai-p2sh (or close to it)
  Sep 1  (932ec7a)       "fixed production of thunder channels, must review unlock now"
```

**Verdict: toolkit is a FORK of ai-p2sh, taken further.** It was NOT the other way around.
The ai-p2sh branch was the development branch, the toolkit was forked from it on Aug 29
(6 days after the last ai-p2sh commit), and then had further development on Sep 1.

The stash on ai-p2sh contains some changes that ALSO appear in toolkit (the multi-input
refactor, the thunderChannel rename), suggesting the stash was either:
- Created around the same time as the toolkit fork (parallel work), OR
- Created after looking at toolkit changes and starting to backport them

### File-by-file: toolkit vs ai-p2sh

#### Files that are IDENTICAL:
- `channels/lock.py` — same
- `channels/unlock.py` — same
- `mining/lock.py` — same
- `mining/unlock.py` — same
- `multisig/lock.py` — same
- `multisig/unlock.py` — same
- `p2pkh/unlock.py` — same
- `utils/sign.py` — same
- `utils/valid.py` — same
- `utils/verify.py` — same
- `identity.py` — same
- `__init__.py` — same
- `walletsh.py` — same

#### `scripts/__init__.py` — DIFFERS (same as stash)
Toolkit has the thunder channel rename that was also in the stash:
- `renewableThunderChannel` → `thunderChannel`
- `nonrenewableThunderChannel` → `thunderExpiring`

#### `utils/multisig.py` — DIFFERS (same as stash)
- Added `Any` import, `### CREATE ###` header
- `dict[dt.datetime, dict]` → `dict[Any, dict]` type fix

#### `evrmore/wallet.py` — MAJOR DIFFERENCES (1077 → 1439 lines, +362 lines)

Toolkit has **all the stash changes** (multi-input, rename) PLUS massive new code:

**Shared with stash (multi-input P2SH refactor):**
- `_compileClaimOnP2SHMultiSigStart()`: single → list params (`fundingTxIds`, `fundingVouts`, `dates`)
- `_compileClaimOnP2SHMultiSigMiddle()`: added `redeemCount` param
- Parameter rename `address` → `toAddress`
- `changeAddress` param added to `_compileCurrencyChangeOutput` and `_compileSatoriChangeOutput`

**Toolkit-only additions (not in stash or ai-p2sh):**

1. New imports: `functools.partial`, `time`, `Any`, `Validate`

2. **`produceThunderChannel()`** (~50 lines) — Creates/funds a thunder channel:
   - Generates redeem script (thunderChannel or thunderExpiring)
   - Handles both Satori asset and currency channels
   - Fee estimation with recursive retry
   - Broadcasts funding tx

3. **`produceThunderChannelFromScript()`** (~50 lines) — Funds channel from pre-built script payload

4. **`produceThunderChannelCurrencyFromScript()`** (~50 lines) — Currency variant

5. **`produceThunderExpiringFromScript()`** — Alias for expiring channels
6. **`produceThunderExpiringCurrencyFromScript()`** — Currency alias

7. **`thunderChannelTransaction()`** (~45 lines) — Claim/unlock thunder channel:
   - Dispatches to recall (single-sig) or multisig based on params
   - Validates amounts and addresses

8. **`thunderChannelRecallTransaction()`** (~50 lines) — Single-sig channel recall:
   - Uses `mining.unlock.multiTimeMultisig` for redeem params
   - Fee estimation with retry

9. **`thunderChannelMultisigTransactionStart()`** (~35 lines) — Multi-sig claim start
10. **`thunderChannelMultisigTransactionMiddle()`** — Create signature for input
11. **`thunderChannelMultisigTransactionEnd()`** (~35 lines) — Finalize with all signatures

12. **`thunderChannelCurrencyTransaction()`** (~30 lines) — Currency claim dispatcher
13. **`thunderChannelRecallCurrencyTransaction()`** (~50 lines) — Currency single-sig recall
14. **`thunderChannelMultisigCurrencyTransactionStart()`** (~35 lines)
15. **`thunderChannelMultisigCurrencyTransactionMiddle()`** — Currency signature
16. **`thunderChannelMultisigCurrencyTransactionEnd()`** (~35 lines) — Currency finalize

**What toolkit REMOVED from ai-p2sh (replaced with the above):**
- `createP2SHTransaction()` — was an unused example with a `raise Exception` at the end
- `p2shFlow()` — was example/pseudocode showing multi-sig flow
- `generatePaymentChannel()` — prototype channel funding
- `generateCommitmentTx()` — prototype commitment tx with dust handling
- `finaliseCommitmentTx()` — prototype Bob-finalizes flow

So the ai-p2sh branch had **prototype/example P2SH methods** and toolkit **replaced them
with real, working implementations**.

#### `wallet/wallet.py` (base class) — DIFFERS (2785 vs 2786 lines, ~200 lines of diff)

The toolkit version has a **naming swap** of the Satori-asset and currency transaction methods:

| ai-p2sh name | toolkit name | What it does |
|---|---|---|
| `multiTimeMultisigTransaction` | `multiTimeMultisigCurrencyTransaction` | Currency claim |
| `multiTimeNotMultisigTransaction` | `multiTimeNotMultisigCurrencyTransaction` | Currency single-sig |
| `multiTimeMultisigTransactionStart` | `multiTimeMultisigCurrencyTransactionStart` | Currency multi-sig start |
| `multiTimeMultisigTransactionMiddle` | `multiTimeMultisigCurrencyTransactionMiddle` | Currency multi-sig middle |
| `multiTimeMultisigTransactionEnd` | `multiTimeMultisigCurrencyTransactionEnd` | Currency multi-sig end |
| `multiTimeMultisigCurrencyTransaction` | `multiTimeMultisigTransaction` | Satori asset claim |
| `multiTimeNotMultisigCurrencyTransaction` | `multiTimeNotMultisigTransaction` | Satori asset single-sig |
| `multiTimeMultisigCurrencyTransactionStart` | `multiTimeMultisigTransactionStart` | Satori asset multi-sig start |
| `multiTimeMultisigCurrencyTransactionMiddle` | `multiTimeMultisigTransactionMiddle` | Satori asset middle |
| `multiTimeMultisigCurrencyTransactionEnd` | `multiTimeMultisigTransactionEnd` | Satori asset end |

**The names were SWAPPED** — ai-p2sh had them backwards (the "Currency" suffix was on the
Satori asset methods, and vice versa). Toolkit fixed the naming.

Additionally, toolkit's Satori-asset methods (the ones without "Currency") now gather
currency UTXOs for fees and include currency change outputs. This makes sense: even when
claiming Satori assets from a P2SH channel, you still need EVR for the transaction fee.

---

## Key Observations (Updated)

1. **satorilib already has some ai-p2sh improvements** — The identity refactor, sign.py changes, and
   wallet constructor refactor in satorilib match what's on ai-p2sh. Someone partially merged these.

2. **satorilib is MISSING the entire p2sh/thunder infrastructure** from all branches.

3. **Lineage: ai-p2sh → stash → toolkit** — ai-p2sh was the development branch. The stash
   started a refactor (multi-input, renames). The toolkit was forked and taken much further,
   replacing prototype code with real working implementations.

4. **ai-p2sh had PROTOTYPE code** — `createP2SHTransaction`, `generatePaymentChannel`,
   `generateCommitmentTx`, `finaliseCommitmentTx` were examples/pseudocode with comments like
   "this is unused" and `raise Exception("don't use it")`.

5. **toolkit has the REAL implementations** — 16 thunder channel methods (produce, claim,
   recall, multisig start/middle/end — for both Satori assets and currency).

6. **toolkit fixed a naming bug** — The Satori vs Currency method names were swapped in
   ai-p2sh. Toolkit corrected them.

7. **The "most complete" version for thunder channels is toolkit.** To get satorilib fully
   updated, we should bring in the toolkit's evrmore/wallet.py thunder methods, the
   scripts/ package, and the utils/ package, plus the base wallet.py naming fix.

---

## Step 4: Porting Plan — toolkit → satorilib

### Guiding Principle
satorilib is the canonical repo. It has improvements toolkit doesn't (RPC support, constructor
cleanup, OTP). We ADD toolkit's thunder code to satorilib — we do NOT overwrite satorilib's
existing improvements.

### Task List (in order of execution)

#### Task 5a: Add `scripts/` package (NEW directory)
Copy from toolkit. These files are identical across toolkit and ai-p2sh, pure additions.
- `scripts/__init__.py` (use toolkit version with `thunderChannel`/`thunderExpiring` names)
- `scripts/channels/__init__.py`
- `scripts/channels/lock.py` (171 lines — thunder channel locking scripts)
- `scripts/channels/unlock.py` (30 lines)
- `scripts/mining/__init__.py`
- `scripts/mining/lock.py` (412 lines — timelock mining scripts)
- `scripts/mining/unlock.py` (68 lines)
- `scripts/multisig/__init__.py`
- `scripts/multisig/lock.py` (29 lines — basic multisig)
- `scripts/multisig/unlock.py` (47 lines)
- `scripts/p2pkh/__init__.py`
- `scripts/p2pkh/unlock.py` (10 lines)

**Risk: None** — purely additive, no conflicts.

#### Task 5b: Add `utils/` package (NEW directory)
- `utils/__init__.py`
- `utils/multisig.py` (250 lines — MultisigUtils class) — use toolkit version (has `Any` fix)
- `utils/sign.py` (29 lines) — toolkit version has `signForPubkey()` that satorilib's
  top-level `sign.py` doesn't have
- `utils/valid.py` — identical to satorilib's top-level `valid.py`
- `utils/verify.py` — identical to satorilib's top-level `verify.py` (just trailing newline)

**Risk: Low** — purely additive. The top-level `sign.py`, `valid.py`, `verify.py` can
stay in place for now (satorilib's `__init__.py` imports from them). We may want to
consolidate later.

#### Task 5c: Update `evrmore/__init__.py`
Current satorilib:
```python
from satorilib.wallet.evrmore.verify import verify
from satorilib.wallet.evrmore.sign import signMessage
from satorilib.wallet.evrmore.wallet import EvrmoreWallet
```
Target (toolkit style, but keeping satorilib's import paths working):
```python
from satorilib.wallet.evrmore.verify import verify
from satorilib.wallet.evrmore.sign import signMessage
from satorilib.wallet.evrmore.wallet import EvrmoreWallet
from satorilib.wallet.evrmore import scripts

__all__ = [
    "verify",
    "signMessage",
    "EvrmoreWallet",
    "scripts",
]
```
We keep imports from top-level `sign.py`/`verify.py` (satorilib's structure) but add the
`scripts` import and `__all__`. We do NOT switch to `utils/` import paths yet to avoid
breaking anything.

**Risk: Low** — additive change.

#### Task 5d: Add thunder channel methods to `evrmore/wallet.py`
This is the big one. We need to ADD toolkit's ~362 new lines to satorilib's wallet.py.

Specifically, add these methods (from toolkit) **without modifying** existing satorilib code:

**New imports needed:**
- `from functools import partial`
- `from typing import Any` (add to existing import)
- `import time`
- `from satorilib.wallet.utils.validate import Validate`

**Multi-input P2SH refactor (modify existing methods):**
- `_compileClaimOnP2SHMultiSigStart()` — change params to lists
- `_compileClaimOnP2SHMultiSigMiddle()` — add `redeemCount`
- `_compileCurrencyChangeOutput()` — add `changeAddress` param
- `_compileSatoriChangeOutput()` — add `changeAddress` param

**New thunder methods to ADD:**
- `produceThunderChannel()`
- `produceThunderChannelFromScript()`
- `produceThunderChannelCurrencyFromScript()`
- `produceThunderExpiringFromScript()`
- `produceThunderExpiringCurrencyFromScript()`
- `thunderChannelTransaction()`
- `thunderChannelRecallTransaction()`
- `thunderChannelMultisigTransactionStart()`
- `thunderChannelMultisigTransactionMiddle()`
- `thunderChannelMultisigTransactionEnd()`
- `thunderChannelCurrencyTransaction()`
- `thunderChannelRecallCurrencyTransaction()`
- `thunderChannelMultisigCurrencyTransactionStart()`
- `thunderChannelMultisigCurrencyTransactionMiddle()`
- `thunderChannelMultisigCurrencyTransactionEnd()`

**REMOVE (present in satorilib, inherited from ai-p2sh prototype):**
- `createP2SHTransaction()` — example code with `raise Exception`
- `p2shFlow()` — pseudocode examples

**Risk: Medium** — need to be careful with the multi-input refactor on methods
that satorilib already modified (identity pattern, etc.). Should review line by line.

#### Task 5e: Fix naming in base `wallet/wallet.py`
The Satori vs Currency method name swap. This touches ~10 method names.

**Risk: Medium** — need to check if anything in satorilib calls these methods by the
old (wrong) names. If so, those callers need updating too.

#### Task 5f: Add `signForPubkey()` to sign.py
Toolkit's `utils/sign.py` has a `signForPubkey()` function that satorilib's `sign.py` lacks.
Add it to satorilib's top-level `sign.py`.

**Risk: None** — purely additive.

### Suggested Order of Execution
1. **5a** (scripts/) — no dependencies, purely additive
2. **5b** (utils/) — no dependencies, purely additive
3. **5f** (signForPubkey) — small, no dependencies
4. **5c** (__init__.py) — depends on 5a existing
5. **5d** (evrmore/wallet.py thunder methods) — the main event
6. **5e** (base wallet.py naming fix) — do last, needs most careful review
