"""
RPC Methods for Wallet - Direct RPC as alternative to ElectrumX

This module provides RPC-based alternatives to ElectrumX methods:
- getReadyToSendRPC() - alternative to getReadyToSend()
- broadcastRPC() - alternative to broadcast()

Usage in wallet subclass:
    from satorilib.wallet.rpc_methods import RpcMethodsMixin

    class EvrmoreWallet(Wallet, RpcMethodsMixin):
        pass
"""
from typing import Union, List, Dict
from decimal import Decimal
from satorilib import logging
from satorilib.rpc import EvermoreRpcClient, UTXO, RpcClientError
from satorilib.wallet.utils.transaction import TxUtils


class RpcMethodsMixin:
    """
    Mixin to add RPC methods to Wallet class

    Requires the class to have:
    - self.address (str)
    - self.unspentCurrency (list)
    - self.unspentAssets (list)
    - self.watchAssets (list)
    """

    def _initRpcClient(
        self,
        rpcNodes: Union[List[Dict[str, str]], None] = None,
        rpcUrl: Union[str, None] = None,
        rpcUser: Union[str, None] = None,
        rpcPassword: Union[str, None] = None,
    ):
        """
        Initialize RPC client (call from __init__ if RPC config provided)

        Args:
            rpcNodes: List of node configs [{"url": ..., "user": ..., "password": ...}]
            rpcUrl: Single node URL (alternative to rpcNodes)
            rpcUser: RPC username (used with rpcUrl)
            rpcPassword: RPC password (used with rpcUrl)
        """
        # Build nodes list from either format
        if rpcNodes:
            nodes = rpcNodes
        elif rpcUrl:
            nodes = [{
                "url": rpcUrl,
                "user": rpcUser or "",
                "password": rpcPassword or "",
            }]
        else:
            # No RPC config - RPC methods won't work but that's ok
            self.rpcClient = None
            return

        try:
            self.rpcClient = EvermoreRpcClient(
                nodes=nodes,
                init_retry=0.25,
                max_retry=4.0,
                timeout=30.0,
                utxo_mode="scantxoutset",
            )
        except Exception as e:
            logging.warning(f"Failed to initialize RPC client: {e}")
            self.rpcClient = None

    def _utxoToElectrumxFormat(self, utxo: UTXO, currentHeight: int) -> dict:
        """
        Convert RPC UTXO to ElectrumX format

        RPC format:
            UTXO(txid='...', vout=0, amount=Decimal('1.5'),
                 confirmations=10, scriptPubKey='76a914...')

        ElectrumX format:
            {'tx_hash': '...', 'tx_pos': 0, 'value': 150000000,
             'height': 868584, 'asset': None}
        """
        # Calculate height from confirmations
        # confirmations = current_height - utxo_height + 1
        # Therefore: utxo_height = current_height - confirmations + 1
        height = currentHeight - utxo.confirmations + 1 if utxo.confirmations > 0 else 0

        # Convert Decimal amount to satoshis (int)
        value = int(utxo.amount * Decimal('100000000'))

        return {
            'tx_hash': utxo.txid,
            'tx_pos': utxo.vout,
            'value': value,
            'height': height,
            'asset': None,  # Currency UTXO (no asset)
        }

    def _utxoToElectrumxAssetFormat(self, utxo: UTXO, currentHeight: int, assetName: str) -> dict:
        """
        Convert RPC UTXO to ElectrumX asset format

        Similar to currency format but includes 'asset' field
        """
        height = currentHeight - utxo.confirmations + 1 if utxo.confirmations > 0 else 0
        value = int(utxo.amount * Decimal('100000000'))

        return {
            'tx_hash': utxo.txid,
            'tx_pos': utxo.vout,
            'value': value,
            'height': height,
            'asset': assetName,
        }

    def getUnspentsRPC(self, min_conf: int = 0):
        """
        Get unspents via RPC (alternative to getUnspents via ElectrumX)

        Populates:
        - self.unspentCurrency - Currency UTXOs (no asset)
        - self.unspentAssets - Asset UTXOs (with asset name)

        This matches the format that ElectrumX provides.
        """
        if not hasattr(self, 'rpcClient') or self.rpcClient is None:
            logging.warning("RPC client not initialized. Use _initRpcClient() first.")
            return

        try:
            # Get all UTXOs for this address
            all_utxos = self.rpcClient.get_unspents(
                addresses=[self.address],
                min_conf=min_conf
            )

            # Get current block height for height calculation
            current_height = self.rpcClient.get_block_count()

            # Filter into currency and assets
            # Note: RPC scantxoutset returns all UTXOs together
            # We need to separate currency from assets
            # For now, treat all as currency (asset detection would require
            # parsing scriptPubKey for OP_EVR_ASSET)

            self.unspentCurrency = [
                self._utxoToElectrumxFormat(utxo, current_height)
                for utxo in all_utxos
            ]

            # Asset filtering would require additional RPC calls or
            # scriptPubKey parsing to detect OP_EVR_ASSET
            # For now, leave assets empty - this handles currency transactions
            self.unspentAssets = []

            logging.debug(f"RPC: Found {len(self.unspentCurrency)} currency UTXOs")

        except RpcClientError as e:
            logging.error(f"RPC error getting unspents: {e.message}")
            raise
        except Exception as e:
            logging.error(f"Error getting unspents via RPC: {e}")
            raise

    def getUnspentTransactionsRPC(self, threaded: bool = False) -> bool:
        """
        Get unspent transactions via RPC (alternative to getUnspentTransactions via ElectrumX)

        Populates self.transactions with full raw transaction data using getrawtransaction RPC.
        This is REQUIRED for transaction building!
        """
        if not hasattr(self, 'rpcClient') or self.rpcClient is None:
            logging.warning("RPC client not initialized")
            return False

        try:
            from satorilib.wallet.concepts.transaction import TransactionStruct

            transactionIds = {tx.txid for tx in self.transactions}
            txids = [uc['tx_hash'] for uc in self.unspentCurrency] + [ua['tx_hash'] for ua in self.unspentAssets]

            for txid in txids:
                if txid not in transactionIds:
                    # Use getrawtransaction with verbose=True to get decoded transaction
                    raw = self.rpcClient.rpc_call("getrawtransaction", [txid, True])
                    logging.debug(f'RPC: Pulling transaction: {txid}')

                    if raw is not None:
                        self.transactions.append(TransactionStruct(
                            raw=raw,
                            vinVoutsTxids=[
                                vin.get('txid', '')
                                for vin in raw.get('vin', [])
                                if vin.get('txid', '') != ''
                            ]
                        ))

            return True

        except Exception as e:
            logging.error(f'Error getting unspent transactions via RPC: {e}')
            return False

    def getUnspentSignaturesRPC(self, force: bool = False) -> bool:
        """
        Get unspent signatures via RPC (alternative to getUnspentSignatures via ElectrumX)

        Adds 'scriptPubKey' to each unspent by extracting from self.transactions.
        This is REQUIRED for transaction building!
        """
        # Check unspents that need scriptPubKey
        if 'SATORIEVR' in self.watchAssets:
            unspents = [
                u for u in self.unspentCurrency + self.unspentAssets
                if 'scriptPubKey' not in u
            ]
        else:
            unspents = [
                u for u in self.unspentCurrency
                if 'scriptPubKey' not in u
            ]

        if not force and len(unspents) == 0:
            # Already have them all
            return True

        try:
            # Add scriptPubKey to each unspent from transactions
            for uc in self.unspentCurrency:
                if uc.get('scriptPubKey', None) is not None:
                    continue

                # Find the transaction
                tx = [tx for tx in self.transactions if tx.txid == uc['tx_hash']]
                if len(tx) > 0:
                    # Find the output
                    vout = [
                        vout for vout in tx[0].raw.get('vout', [])
                        if vout.get('n') == uc['tx_pos']
                    ]
                    if len(vout) > 0:
                        scriptPubKey = vout[0].get('scriptPubKey', {}).get('hex', None)
                        if scriptPubKey is not None:
                            uc['scriptPubKey'] = scriptPubKey

            # Same for assets
            if 'SATORIEVR' in self.watchAssets:
                for ua in self.unspentAssets:
                    if ua.get('scriptPubKey', None) is not None:
                        continue

                    tx = [tx for tx in self.transactions if tx.txid == ua['tx_hash']]
                    if len(tx) > 0:
                        vout = [
                            vout for vout in tx[0].raw.get('vout', [])
                            if vout.get('n') == ua['tx_pos']
                        ]
                        if len(vout) > 0:
                            scriptPubKey = vout[0].get('scriptPubKey', {}).get('hex', None)
                            if scriptPubKey is not None:
                                ua['scriptPubKey'] = scriptPubKey

            return True

        except Exception as e:
            logging.error(f'Error getting unspent signatures via RPC: {e}')
            return False

    def getReadyToSendRPC(self, balance: bool = False, save: bool = True):
        """
        Get ready to send via RPC (alternative to getReadyToSend via ElectrumX)

        This is the COMPLETE RPC equivalent of getReadyToSend():
        - getUnspents() -> getUnspentsRPC()
        - getUnspentTransactions() -> getUnspentTransactionsRPC()
        - getUnspentSignatures() -> getUnspentSignaturesRPC()

        Populates ALL the same memory structures as ElectrumX version!
        """
        if not hasattr(self, 'rpcClient') or self.rpcClient is None:
            logging.warning("RPC client not initialized")
            return

        try:
            if balance:
                # Could implement getBalancesRPC if needed
                logging.debug("Balance query not yet implemented via RPC")

            # Step 1: Get unspents via RPC
            self.getUnspentsRPC()

            # Step 2: Get full transaction data via RPC
            # This populates self.transactions - REQUIRED!
            self.getUnspentTransactionsRPC(threaded=False)

            # Step 3: Add scriptPubKey to unspents
            # This is REQUIRED for transaction building!
            self.getUnspentSignaturesRPC()

            if save:
                self.saveCache()

        except Exception as e:
            logging.error(f'Unable to get ready to send via RPC: {e}')

    def broadcastRPC(self, txHex: str) -> str:
        """
        Broadcast transaction via RPC (alternative to broadcast via ElectrumX)

        Args:
            txHex: Hex-encoded signed transaction

        Returns:
            Transaction ID (txid)

        Raises:
            RpcClientError: If broadcast fails
        """
        if not hasattr(self, 'rpcClient') or self.rpcClient is None:
            raise Exception("RPC client not initialized")

        try:
            txid = self.rpcClient.broadcast_raw_tx(txHex)
            logging.debug(f"RPC broadcast successful: {txid}")
            return txid
        except RpcClientError as e:
            logging.error(f"RPC broadcast failed: {e.message}")
            raise
        except Exception as e:
            logging.error(f"Error broadcasting via RPC: {e}")
            raise


# Utility function for standalone usage
def createRpcClient(
    url: str,
    user: str,
    password: str,
    **kwargs
) -> EvermoreRpcClient:
    """
    Create an RPC client for standalone use

    Args:
        url: RPC node URL (e.g., "http://localhost:8820")
        user: RPC username
        password: RPC password
        **kwargs: Additional EvermoreRpcClient parameters

    Returns:
        Configured EvermoreRpcClient instance

    Example:
        rpc = createRpcClient(
            url="http://localhost:8820",
            user="rpcuser",
            password="rpcpass"
        )
        utxos = rpc.get_unspents(["EVRaddress1"])
        txid = rpc.broadcast_raw_tx("01000000...")
    """
    return EvermoreRpcClient(
        nodes=[{"url": url, "user": user, "password": password}],
        init_retry=kwargs.get('init_retry', 0.25),
        max_retry=kwargs.get('max_retry', 4.0),
        timeout=kwargs.get('timeout', 30.0),
        utxo_mode=kwargs.get('utxo_mode', 'scantxoutset'),
        verify_tls=kwargs.get('verify_tls', True),
    )
