"""Utility functions for parsing SATORI asset transactions."""

from typing import TypedDict, List


class SatoriOutput(TypedDict):
    """SATORI output structure."""
    address: str
    amount: float
    n: int  # output index
    spent: bool
    spent_txid: str | None


class SatoriInput(TypedDict):
    """SATORI input structure (from previous tx output)."""
    address: str
    amount: float
    txid: str
    vout: int


class SatoriTransaction(TypedDict):
    """Parsed SATORI transaction."""
    txid: str
    height: int
    time: int
    inputs: List[SatoriInput]
    outputs: List[SatoriOutput]


def parse_satori_transaction(tx: dict) -> SatoriTransaction:
    """Extract SATORI inputs and outputs from a transaction.

    Args:
        tx: Transaction dict from ElectrumX (must include verbose output)

    Returns:
        SatoriTransaction with parsed SATORI inputs and outputs

    Note:
        Input amounts are only available if the tx dict includes the 'vin'
        entries with asset information. Standard ElectrumX responses may
        not include this - you may need to fetch previous transactions
        to get input amounts.
    """
    outputs = []
    inputs = []

    # Parse outputs
    for vout in tx.get('vout', []):
        script_pubkey = vout.get('scriptPubKey', {})
        asset = script_pubkey.get('asset')

        if asset and asset.get('name') == 'SATORI':
            addresses = script_pubkey.get('addresses', [])
            address = addresses[0] if addresses else None

            outputs.append({
                'address': address,
                'amount': asset.get('amount', 0.0),
                'n': vout.get('n'),
                'spent': 'spentTxId' in vout,
                'spent_txid': vout.get('spentTxId')
            })

    # Parse inputs (if asset info is available)
    # Note: Standard vin entries don't include asset amounts
    # This would require fetching the previous transaction outputs
    for vin in tx.get('vin', []):
        # Check if this vin has asset information (non-standard)
        if 'asset' in vin and vin['asset'].get('name') == 'SATORI':
            inputs.append({
                'address': vin.get('address'),
                'amount': vin['asset'].get('amount', 0.0),
                'txid': vin.get('txid'),
                'vout': vin.get('vout')
            })

    return {
        'txid': tx.get('txid'),
        'height': tx.get('height'),
        'time': tx.get('time') or tx.get('blocktime'),
        'inputs': inputs,
        'outputs': outputs
    }


def get_satori_balance_change(tx: dict, address: str) -> float:
    """Calculate net SATORI balance change for an address in a transaction.

    Args:
        tx: Transaction dict from ElectrumX
        address: Address to calculate balance change for

    Returns:
        Net change in SATORI balance (positive = received, negative = sent)

    Note:
        Input amounts are only calculated if available in tx dict.
    """
    parsed = parse_satori_transaction(tx)

    # Sum outputs to this address
    received = sum(
        out['amount']
        for out in parsed['outputs']
        if out['address'] == address
    )

    # Sum inputs from this address (if available)
    sent = sum(
        inp['amount']
        for inp in parsed['inputs']
        if inp['address'] == address
    )

    return received - sent
