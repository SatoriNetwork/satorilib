from typing import Union
from evrmore.wallet import CEvrmoreSecret
from evrmore.signmessage import signMessage as sm
from evrmore.signmessage import EvrmoreMessage
from satorilib.wallet.cryptolock import WALLET_CRYPTO_LOCK


def signMessage(key: CEvrmoreSecret, message: Union[str, EvrmoreMessage]):
    ''' returns binary signature '''
    # Native (OpenSSL/libsecp256k1) signing is not thread-safe; serialize it.
    with WALLET_CRYPTO_LOCK:
        return sm(
            key,
            EvrmoreMessage(message) if isinstance(message, str) else message)

def sign(privkey: Union[str, bytes], message: Union[str, EvrmoreMessage]):
    ''' returns binary signature '''
    return SignMessage(
        CEvrmoreSecret(privkey),
        EvrmoreMessage(message) if isinstance(message, str) else message)
