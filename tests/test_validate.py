from satorilib.wallet.utils.validate import Validate

CHECKSUMMED = '0x52908400098527886E0F7030069857D2E4169EE7'


def test_ethAddress_accepts_checksummed_and_single_case():
    assert Validate.ethAddress(CHECKSUMMED)
    assert Validate.ethAddress(CHECKSUMMED.lower())


def test_ethAddress_rejects_a_bad_checksum():
    assert not Validate.ethAddress('0x52908400098527886E0F7030069857D2E4169Ee7')


def test_ethAddress_rejects_non_hex_and_wrong_length():
    assert not Validate.ethAddress('0x' + 'g' * 40)
    assert not Validate.ethAddress(CHECKSUMMED[:-1])
    assert not Validate.ethAddress('EKzP3RXHvcLWZ8EnqW3J1WXsDNXQ2ybWtL')
