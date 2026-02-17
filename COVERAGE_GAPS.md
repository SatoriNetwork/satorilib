# Coverage Gaps

## 1. relay.py — 89% (7 lines remaining) [DONE]

20 tests in test_relay.py. Remaining 7 lines are `except` blocks in
nostr-sdk calls (L125-129 connect error, L140-142 disconnect error)
that can't be triggered without mocking SDK internals.

## 2. client.py — 92% (23 lines remaining) [DONE]

46 tests in test_client.py. Remaining 23 lines are all `except` error-handling
blocks and internal handler edge cases:
- L342-343: Per-subscriber send error (except block)
- L452-453, L733-734: Metadata parse error (except blocks)
- L835, L859-860: _event_listener early return + error (except block)
- L871: _handle_event dedupe return (timing-dependent)
- L899-902: Decrypt fallback (needs crafted encrypted event)
- L914-917, L948-951, L967-968: Handler error branches (except blocks)

## 3. reliable_subscriber.py — 86% (24 lines) [SKIP]

All remaining lines are except blocks, CancelledError handlers, and
background task internals. Only real logic is `_auto_pay` but ROI too
low (complex paid stream setup for one method).

## 4. stream_monitor.py — 91% (12 lines) [SKIP]

All remaining lines are except blocks in callbacks and monitor loop.

## 5. dedupe.py — 96% (1 line) [SKIP]

L92: `SQLiteDedupe.__init__` raises NotImplementedError.

## 6. encryption.py — 91% (2 lines) [SKIP]

L50-51: Error wrapping in `encrypt_json`.

---

## Final: 211 tests, 92% coverage. Remaining 8% is error-handling plumbing.
