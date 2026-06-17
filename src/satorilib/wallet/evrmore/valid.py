from typing import Union
import hashlib
import base58


def isValidEvrmoreAddressBasic(address: str) -> bool:
    '''Evrmore addresses are 34 chars: P2PKH start with 'E', P2SH with 'e'.'''
    import re
    pattern = r'^[Ee][a-zA-Z0-9]{33}$'
    return bool(re.match(pattern, address))


def base58_check_decode(address: Union[str, bytes]) -> tuple[bool, Union[int, None]]:
    ''' Decode Base58Check address and return the payload and version byte. '''
    try:
        decoded = base58.b58decode(address)
        version = decoded[0]
        checksum = decoded[-4:]
        payload = decoded[:-4]
        # Calculate checksum of the payload
        checksum_check = hashlib.sha256(
            hashlib.sha256(payload).digest()).digest()[:4]
        if checksum != checksum_check:
            return False, None  # Checksum does not match
        return True, version  # Return payload and version byte
    except Exception:
        return False, None


def isValidEvrmoreAddress(address: str) -> bool:
    ''' Validate Evrmore address using Base58Check. '''
    if address is None:
        return False
    if isinstance(address, str) and (len(address) != 34 or address[0] not in ('E', 'e')):
        return False
    try:
        from evrmore.wallet import CEvrmoreAddress
        CEvrmoreAddress(address)
        if not isValidEvrmoreAddressBasic(address):
            return False
        is_valid, version = base58_check_decode(address)
        if not is_valid:
            return False
        # Evrmore P2PKH ('E', version 33/0x21) or P2SH multisig ('e', version 92/0x5c)
        if version == 0x21:    # P2PKH
            return True
        elif version == 0x5c:  # P2SH (multisig) — was wrongly 0x5a
            return True
        else:
            return False  # Invalid version byte
    except Exception:
        return False




def validEvrmoreAddress(address: str) -> Union[str, None]:
    ''' Validate Evrmore address using Base58Check. '''
    if not isValidEvrmoreAddress(address):
        return None
    return address
