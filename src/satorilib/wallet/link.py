'''
The link: the wallet's signed word that an alias acts for it.

The alias is the automated signing key a neuron makes and keeps unencrypted;
the wallet is the person's own key, which holds the funds. The Satori Network
pays the wallet an alias is linked to. The link is an EIP-191 (personal_sign)
signature by the wallet of exactly these four lines (joined by "\\n", no
trailing newline):

    Satori: link this alias to my wallet
    alias: 0x<the alias's address, EIP-55>
    wallet: 0x<the wallet's address, EIP-55>
    issued: <unix seconds>

A register sends it as the headers `wallet-link` (the message, base64 of its
UTF-8, since a header holds one line) and `wallet-signature`. Nothing in a
link is secret.
'''
from typing import Union
import base64
import time

LINK_TITLE = 'Satori: link this alias to my wallet'


def _checksum(address: str) -> str:
    from eth_utils import to_checksum_address
    return to_checksum_address(address)


def linkMessage(aliasAddress: str, walletAddress: str, issued: Union[int, None] = None) -> str:
    ''' the exact text the wallet signs '''
    issued = int(time.time()) if issued is None else int(issued)
    return '\n'.join([
        LINK_TITLE,
        f'alias: {_checksum(aliasAddress)}',
        f'wallet: {_checksum(walletAddress)}',
        f'issued: {issued}'])


def makeLink(wallet, aliasAddress: str, issued: Union[int, None] = None) -> dict:
    '''
    the wallet (unlocked; anything with ethAddress and ethSign) links the alias
    at aliasAddress: {'message', 'signature'}
    '''
    message = linkMessage(aliasAddress, wallet.ethAddress, issued)
    return {'message': message, 'signature': wallet.ethSign(message)}


def linkHeaders(link: Union[dict, None]) -> dict:
    ''' the register headers that carry a link ({} without one) '''
    if not link or not link.get('message') or not link.get('signature'):
        return {}
    return {
        'wallet-link': base64.b64encode(link['message'].encode('utf-8')).decode('ascii'),
        'wallet-signature': link['signature']}


def linkFields(message: str) -> dict:
    ''' {'alias', 'wallet', 'issued'} read from a link message ({} if it is not one) '''
    lines = (message or '').split('\n')
    if len(lines) != 4 or lines[0] != LINK_TITLE:
        return {}
    fields = {}
    for line, name in zip(lines[1:], ('alias', 'wallet', 'issued')):
        prefix = f'{name}: '
        if not line.startswith(prefix):
            return {}
        fields[name] = line[len(prefix):]
    try:
        fields['issued'] = int(fields['issued'])
    except ValueError:
        return {}
    return fields


def linkSigner(link: Union[dict, None]) -> Union[str, None]:
    ''' the address that signed the link, or None '''
    try:
        from eth_account import Account
        from eth_account.messages import encode_defunct
        return Account.recover_message(
            encode_defunct(text=link['message']),
            signature=link['signature'])
    except Exception:
        return None


def linkIsFor(link: Union[dict, None], aliasAddress: str, walletAddress: str) -> bool:
    ''' True when the link is the wallet at walletAddress linking the alias at aliasAddress '''
    if not link:
        return False
    fields = linkFields(link.get('message'))
    if not fields:
        return False
    try:
        alias, wallet = _checksum(aliasAddress), _checksum(walletAddress)
    except Exception:
        return False
    return (
        fields['alias'] == alias
        and fields['wallet'] == wallet
        and linkSigner(link) == wallet)
