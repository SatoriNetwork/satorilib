# a satori node authenticates to central by signing a message (central's challenge,
# or the date in UTC now) and sending its public key, the message and the signature.
# the signature is EIP-191 (Ethereum personal_sign); central recovers the signer and
# compares it with the Ethereum address of wallet-pubkey.
import json
from satorilib.utils.time import nowStr


def authPayload(wallet, challenge: str = None):
    ''' headers for central's legacy auth (see authenticate in central) '''
    challenge = challenge or getFullDateMessage()
    return {
        'message': challenge,
        'wallet-pubkey': wallet.pubkey,
        'signature': wallet.ethSign(challenge)}


def getFullDateMessage():
    ''' returns a string of today's date in UTC like this: "2022-08-01 17:28:44.748691" '''
    return nowStr()


class AuthPayload:
    '''
        {'message': '2023-09-30 04:06:32.908595',
        'pubkey': '021bd7999774a59b6d0e40d650c2ed24a49a54bdb0b46c922fd13afe8a4f3e4aeb',
        'address': 'RTEabwWn7zuTxgjwrryYtZv3ELTUidtdF7',
        'signature': '...'}
    '''

    @staticmethod
    def create(wallet, challenge: str = None) -> 'AuthPayload':
        return AuthPayload(raw=authPayload(wallet, challenge))

    def __init__(self, raw: dict = None):
        self.message: str = raw.get('message')
        self.pubkey: str = raw.get('pubkey')
        self.address: str = raw.get('address')
        self.signature: str = raw.get('signature')

    def __str__(self):
        return (
            'AuthPayload('
            f'\n\tmessage: {self.message},'
            f'\n\tpubkey: {self.pubkey},'
            f'\n\taddress: {self.address},'
            f'\n\tsignature: {self.signature})')

    def toJson(self):
        return json.dumps(self.raw)

    def toDict(self):
        return {
            'message': self.message,
            'pubkey': self.pubkey,
            'address': self.address,
            'signature': self.signature}
