"""Process-wide lock serializing native wallet crypto.

python-evrmore binds OpenSSL and libsecp256k1 through ctypes using
process-global native state (see evrmore/core/key.py). ctypes releases the GIL
during native calls, so two threads doing key construction / signing at the
same time run concurrently in native malloc/free and corrupt the glibc heap ->
SIGABRT "corrupted size vs. prev_size".

Per-object locks don't help because the corrupted state is shared under every
wallet object. Every native crypto section (key construction, signing) must
take this single leaf lock. It is reentrant so a caller may hold it across a
multi-step operation (e.g. a transaction build that also signs) without
deadlocking on the inner guarded primitives.

Import this ONE object everywhere (satorilib and satorineuron) so all callers
share the same lock:

    from satorilib.wallet.cryptolock import WALLET_CRYPTO_LOCK
"""
import threading

WALLET_CRYPTO_LOCK = threading.RLock()
