class Validate():
    ''' heuristics '''

    @staticmethod
    def address(address: str, currency: str) -> bool:
        return (
            (currency.lower() == 'rvn' and (address.startswith('R') or address.startswith('r')) and len(address) == 34) or
            (currency.lower() == 'evr' and (address.startswith('E') or address.startswith('e')) and len(address) == 34)) # supports p2pkh and p2sh

    @staticmethod
    def ethAddress(address: str) -> bool:
        from satorilib.wallet.ethereum.valid import isValidEthereumAddress
        return isValidEthereumAddress(address)
