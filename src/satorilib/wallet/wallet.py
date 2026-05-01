from functools import partial
from typing import Union, Callable, Optional
import os
import datetime as dt
import json
import joblib
import threading
from enum import Enum
from random import randrange
from decimal import Decimal
from satorilib import logging
from satorilib.utils import system
from satorilib.disk.utils import safetify
from satorilib.electrumx import Electrumx
from satorilib.wallet.utils.transaction import TxUtils
from satorilib.wallet.utils.validate import Validate
from satorilib.wallet.concepts.balance import Balance
from satorilib.wallet.concepts.transaction import TransactionResult, TransactionFailure, TransactionStruct
from satorilib.wallet.identity import Identity, IdentityBase


class TxCreationValidation(Enum):
    ready = (1, 'Initial state, ready to create')
    partialReady = (2, 'Partially ready')
    failure = (3, 'Creation failed')

    def __init__(self, code:int, description:str):
        self.code = code
        self.description = description


class WalletBase:
    def __init__(self, identity: IdentityBase):
        self.identity = identity
        
    # Forward identity methods
    def sign(self, message: str) -> bytes:
        return self.identity.sign(message)
        
    def verify(self, message: str, sig: bytes, address: Union[str, None] = None) -> bool:
        return self.identity.verify(message, sig, address)
        
    @property
    def address(self) -> str:
        return self.identity.address
        
    @property
    def pubkey(self) -> str:
        return self.identity.pubkey
        
    @property
    def privkey(self) -> str:
        return self.identity.privkey
        
    @property
    def words(self) -> str:
        return self.identity.words
        
    @property
    def scripthash(self) -> str:
        return self.identity.scripthash

    def hash160ToAddress(self, pubKeyHash: Union[str, bytes]) -> str:
        return self.identity.hash160ToAddress(pubKeyHash)

    def authenticationPayload(
        self,
        challengeId: Union[str, None] = None,
        challenged: Union[str, None] = None,
        signature: Union[bytes, None] = None,
    ) -> dict[str, str]:
        return self.identity.authenticationPayload(
            challengeId=challengeId,
            challenged=challenged,
            signature=signature)

    # Additional helper properties
    @property
    def publicKeyBytes(self) -> bytes:
        return self.identity.publicKeyBytes

    @property
    def isEncrypted(self) -> bool:
        return self.identity.isEncrypted

    @property
    def isDecrypted(self) -> bool:
        return self.identity.isDecrypted

    @property
    def networkByte(self) -> bytes:
        return self.identity.networkByte

    @property
    def symbol(self) -> str:
        return self.identity.symbol

    def close(self) -> None:
        """Forward close operation to identity"""
        self.identity.close()

    def open(self, password: Union[str, None] = None) -> None:
        self.identity.open(password)

    def loadFromYaml(self, yaml: Union[dict, None] = None) -> None:
        """Forward YAML loading to identity"""
        self.identity.loadFromYaml(yaml)

    def validateIdentity(self) -> bool:
        """Forward identity validation"""
        return self.identity.validateIdentity()

    def setAlias(self, alias: Union[str, None] = None) -> None:
        self.identity.setAlias(alias)

    def authPayload(self, asDict: bool = False, challenge: str = None) -> Union[str, dict]:
        return self.identity.authPayload(asDict=asDict, challenge=challenge)

    def saveAsVault(self, password: str, vaultPath: str = None) -> None:
        vaultPath = vaultPath or self.walletPath.replace('wallet-', 'vault-')
        self.identity.password = password
        self.identity.save(path=vaultPath)


class Wallet(WalletBase):

    @staticmethod
    def openSafely(
        supposedDict: Union[dict, None],
        key: str,
        default: Union[str, int, dict, list, None] = None,
    ):
        if not isinstance(supposedDict, dict):
            return default
        try:
            return supposedDict.get(key, default)
        except Exception as e:
            logging.error('openSafely err:', supposedDict, e)
            return default

    def __init__(
        self,
        walletPath: Union[str,None] = None,
        password: Union[str,None] = None,
        electrumx: Union[Electrumx, None] = None,
        cachePath: Union[str,None] = None,
        identity: Union[Identity, None] = None,
        reserve: float = 0.25,
        watchAssets: list[str] = None,
        skipSave: bool = False,
        pullFullTransactions: bool = True,
        balanceUpdatedCallback: Union[Callable, None] = None,
        scriptStatusCallback: Union[Callable, None] = None,
    ):
        if walletPath == cachePath and walletPath is not None:
            raise Exception('wallet and cache paths cannot be the same')
        if identity is None and walletPath is None:
            raise Exception('wallet path or identity is required')
        elif identity is not None and walletPath is not None:
            raise Exception('wallet path and identity cannot be provided together')
        elif identity is None and walletPath is not None:
            identity = Identity(walletPath=walletPath, password=password)
        super().__init__(identity=identity)
        self.skipSave = skipSave
        self.watchAssets = ['SATORI'] if watchAssets is None else watchAssets
        # at $100 SATORI this is 1 penny (for evr tx fee)
        self.mundoFee = 0.0001
        # at $100 SATORI this is 1 dollar (for risk and gas fees), by BurnBridge
        self.bridgeFee: float = 0.01
        self.bridgeAddress: str = 'EUqCW1WmT6a9Y6RBVhsxY1k4S135RPWCy7'  # TODO finish
        self.burnAddress: str = TxUtils.evrBurnMintAddressMain #'EXBurnMintXXXXXXXXXXXXXXXXXXXbdK5E'  # real
        self.maxBridgeAmount: float = 500
        #self.burnAddress: str = 'EL1BS6HmwY1KoeqBokKjUMcWbWsn5kamGv' # testing
        self.password = password
        self.walletPath = identity.walletPath or walletPath
        self.cachePath = cachePath or self.walletPath.replace('.yaml', '.cache.joblib')
        # maintain minimum amount of currency at all times to cover fees - server only
        self.reserveAmount = reserve
        self.reserve = TxUtils.asSats(reserve)
        self.stats = {}
        self.alias = None
        self.banner = None
        self.currency: Balance = Balance.empty('EVR')
        self.balance: Balance = Balance.empty('SATORI')
        self.divisibility = 8
        self.transactionHistory: list[dict] = []
        # TransactionStruct(*v)... {txid: (raw, vinVoutsTxs)}
        self._transactions: dict[str, tuple[dict, list[dict]]] = {}
        self.cache = {}
        self.transactions: list[TransactionStruct] = []
        self.assetTransactions = []
        self.electrumx: Electrumx = electrumx # type: ignore
        self.unspentCurrency = None
        self.unspentAssets = None
        self.status = None
        self.pullFullTransactions = pullFullTransactions
        self.lastBalanceAmount = 0
        self.lastCurrencyAmount = 0
        self.balanceUpdatedCallback = balanceUpdatedCallback
        self.scriptStatusCallback = scriptStatusCallback
        self.loadCache()
        self.scriptsPath = self.walletPath.replace('.yaml', '.scripts.json')
        self.scripts: dict[str, dict] = {}
        self.loadScripts()

    def __call__(self):
        self.get()
        return self

    def __repr__(self):
        return self.identity.__repr__()[:-1] + ',\n' + (
            f'\n  currency: {self.currency},'
            f'\n  balance: {self.balance},'
            f'\n  stats: {self.stats},'
            f'\n  banner: {self.banner})'
        )

    ### Ethereum ################################################################

    @property
    def account(self) -> 'eth_account.Account':
        if self.isDecrypted:
            try:
                from satorilib.wallet.ethereum.wallet import EthereumWallet
                return EthereumWallet.generateAccount(self.identity._entropy)
            except Exception as e:
                logging.error('error with eth account:', e)
        return None

    @property
    def ethAddress(self) -> str:
        return self.account.address

    ### Caching ################################################################

    def cacheFileExists(self):
        return os.path.exists(self.cachePath)

    def loadCache(self) -> bool:

        def fromJoblib(cachePath: str) -> Union[None, dict]:
            try:
                if os.path.isfile(cachePath):
                    return joblib.load(cachePath)
                return None
            except Exception as e:
                logging.debug(f'unable to load wallet cache, creating a new one: {e}')
                if os.path.isfile(cachePath):
                    os.remove(cachePath)
                return None

        if self.skipSave:
            return False
        if not self.cacheFileExists():
            return False
        try:
            if self.cachePath.endswith('.joblib'):
                self.cache = fromJoblib(self.cachePath)
                if self.cache is None:
                    return False
                self.status = self.cache['status']
                self.unspentCurrency = self.cache['unspentCurrency']
                self.unspentAssets = self.cache['unspentAssets']
                self.transactions = self.cache['transactions']
                return self.status
            return False
        except Exception as e:
            logging.error(f'issue loading transaction cache, {e}')
            return False

    def saveCache(self):
        if self.skipSave:
            return False
        try:
            safetify(self.cachePath)
            if self.cachePath.endswith('.joblib'):
                safetify(self.cachePath)
                joblib.dump({
                    'status': self.status,
                    'unspentCurrency': self.unspentCurrency,
                    'unspentAssets': self.unspentAssets,
                    'transactions': self.transactions},
                    self.cachePath)
                return True
        except Exception as e:
            logging.error("wallet transactions saveCache error", e)

    ### P2SH Script Persistence ################################################

    @staticmethod
    def p2shScripthash(p2shAddress: str) -> str:
        ''' compute ElectrumX scripthash for a P2SH address '''
        from base58 import b58decode_check
        from hashlib import sha256
        scriptHash = b58decode_check(p2shAddress)[1:]
        scriptPubKey = bytes.fromhex('a914') + scriptHash + bytes.fromhex('87')
        return sha256(scriptPubKey).digest()[::-1].hex()

    def loadScripts(self) -> bool:
        if self.skipSave:
            return False
        try:
            if os.path.isfile(self.scriptsPath):
                with open(self.scriptsPath, 'r') as f:
                    self.scripts = json.load(f)
                return True
            return False
        except Exception as e:
            logging.error(f'unable to load scripts file: {e}')
            return False

    def saveScripts(self) -> bool:
        if self.skipSave:
            return False
        try:
            safetify(self.scriptsPath)
            tmpPath = self.scriptsPath + '.tmp'
            with open(tmpPath, 'w') as f:
                json.dump(self.scripts, f, indent=2)
            os.replace(tmpPath, self.scriptsPath)
            return True
        except Exception as e:
            logging.error(f'saveScripts error: {e}')
            return False

    def saveScript(self, scriptPayload: dict) -> bool:
        p2shAddress = scriptPayload.get('p2sh_address')
        if not p2shAddress:
            logging.error('saveScript: missing p2sh_address')
            return False
        existing = self.scripts.get(p2shAddress, {})
        now = dt.datetime.utcnow().isoformat() + 'Z'
        merged = {**existing, **scriptPayload}
        if 'created_at' not in existing:
            merged['created_at'] = now
        if scriptPayload.get('funding_txid'):
            merged['status'] = 'funded'
            if 'funded_at' not in existing or existing.get('status') != 'funded':
                merged['funded_at'] = now
        else:
            merged.setdefault('status', 'pending')
        merged['wallet_address'] = self.address
        self.scripts[p2shAddress] = merged
        result = self.saveScripts()
        if merged.get('status') == 'funded' and existing.get('status') != 'funded':
            self._subscribeToP2SHScript(p2shAddress)
        return result

    def getScript(self, p2shAddress: str) -> Union[dict, None]:
        return self.scripts.get(p2shAddress)

    def removeScript(self, p2shAddress: str) -> bool:
        if p2shAddress in self.scripts:
            self.scripts[p2shAddress]['status'] = 'spent'
            return self.saveScripts()
        return False

    def getScriptTimelockInfo(self, p2shAddress: str) -> Union[dict, None]:
        '''
        returns recall timing info for a script:
            type: 'csv' (relative) or 'cltv' (absolute)
            recall_available: bool
            recall_block: absolute block height when recall opens (csv+blocks or cltv+blocks)
            recall_time: datetime when recall opens (csv+minutes or cltv+timestamp)
        requires funding_confirmed_height for csv/blocks calculations.
        '''
        script = self.scripts.get(p2shAddress)
        if script is None:
            return None
        fn = script.get('function', '')
        params = script.get('original_params', {})
        blocks = params.get('blocks')
        minutes = params.get('minutes')
        if fn == 'paymentChannel':
            # CSV — relative timelock from confirmation
            confirmedHeight = script.get('funding_confirmed_height')
            if blocks is not None and confirmedHeight is not None:
                return {
                    'type': 'csv',
                    'unit': 'blocks',
                    'recall_block': confirmedHeight + blocks,
                    'recall_time': None}
            if minutes is not None:
                confirmedAt = script.get('funding_confirmed_at')
                if confirmedAt is not None:
                    if isinstance(confirmedAt, str):
                        confirmedAt = dt.datetime.fromisoformat(
                            confirmedAt.replace('Z', '+00:00'))
                    return {
                        'type': 'csv',
                        'unit': 'time',
                        'recall_block': None,
                        'recall_time': confirmedAt + dt.timedelta(minutes=minutes)}
            return {'type': 'csv', 'unit': 'blocks' if blocks else 'time',
                    'recall_block': None, 'recall_time': None}
        if fn == 'paymentChannelExpiring':
            # CLTV — absolute timelock
            if blocks is not None:
                return {
                    'type': 'cltv',
                    'unit': 'blocks',
                    'recall_block': blocks,
                    'recall_time': None}
            # timestamp was computed from minutes at creation time and stored
            # in the redeem script. We can recover it from original_params.
            if minutes is not None:
                createdAt = script.get('created_at')
                if createdAt is not None:
                    if isinstance(createdAt, str):
                        createdAt = dt.datetime.fromisoformat(
                            createdAt.replace('Z', '+00:00'))
                    return {
                        'type': 'cltv',
                        'unit': 'time',
                        'recall_block': None,
                        'recall_time': createdAt + dt.timedelta(minutes=minutes)}
            return {'type': 'cltv', 'unit': 'blocks' if blocks else 'time',
                    'recall_block': None, 'recall_time': None}
        return None

    def isRecallAvailable(
        self,
        p2shAddress: str,
        currentHeight: Union[int, None] = None,
        currentTime: Union[dt.datetime, None] = None,
    ) -> bool:
        ''' check if the sender recall path is spendable for a script '''
        info = self.getScriptTimelockInfo(p2shAddress)
        if info is None:
            return False
        recallBlock = info.get('recall_block')
        recallTime = info.get('recall_time')
        if recallBlock is not None and currentHeight is not None:
            return currentHeight >= recallBlock
        if recallTime is not None:
            now = currentTime or dt.datetime.now(dt.timezone.utc)
            return now >= recallTime
        return False

    ### Electrumx ##############################################################

    def connected(self) -> bool:
        if isinstance(self.electrumx, Electrumx):
            return self.electrumx.connected()
        return False

    def subscribeToScripthashActivity(self):

        def parseNotification(notification: dict) -> str:
            return notification.get('params',['scripthash', 'status'])[-1]

        def handleNotifiation(notification: dict):
            return updateStatus(parseNotification(notification))

        def handleResponse(status: str):
            return updateStatus(status)

        def updateStatus(status: str) -> bool:
            def thenSave():
                self.getUnspentSignatures()
                self.status = status
                self.saveCache()

            if self.status == status:
                return False
            self.getBalances()
            self.getUnspents()
            self.status = status
            self.getUnspentTransactions(threaded=True, then=thenSave)
            return True

        return handleResponse(
            self.electrumx.api.subscribeScripthash(
                scripthash=self.scripthash,
                callback=handleNotifiation))

    def subscribeToP2SHScripts(self):
        ''' subscribe to all funded/active P2SH scripts for on-chain monitoring '''
        if not self.connected():
            return
        for p2shAddress, script in self.scripts.items():
            if script.get('status') in ('funded', 'active'):
                self._subscribeToP2SHScript(p2shAddress)

    def _subscribeToP2SHScript(self, p2shAddress: str):
        ''' subscribe to a single P2SH script's scripthash '''
        if not self.connected():
            return
        script = self.scripts.get(p2shAddress)
        if script is None:
            return
        scripthash = script.get('scripthash')
        if not scripthash:
            scripthash = self.p2shScripthash(p2shAddress)
            script['scripthash'] = scripthash
            self.saveScripts()

        def handleNotification(notification: dict):
            self._handleP2SHNotification(p2shAddress)

        self.electrumx.api.subscribeScripthash(
            scripthash=scripthash,
            callback=handleNotification)

    def _handleP2SHNotification(self, p2shAddress: str):
        ''' check if funding UTXO still exists, update script status '''
        script = self.scripts.get(p2shAddress)
        if script is None:
            return
        scripthash = script.get('scripthash')
        if not scripthash:
            return
        oldStatus = script.get('status')
        try:
            unspents = (
                self.electrumx.api.getUnspentCurrency(scripthash) or [])
            unspents += (
                self.electrumx.api.getUnspentAssets(scripthash) or [])
        except Exception as e:
            logging.error(f'P2SH notification check failed for {p2shAddress}: {e}')
            return
        fundingTxid = script.get('funding_txid')
        fundingVout = script.get('funding_vout')
        fundingUtxo = next((
            u for u in unspents
            if u.get('tx_hash') == fundingTxid
            and u.get('tx_pos') == fundingVout), None)
        if fundingUtxo is not None:
            if oldStatus == 'funded':
                script['status'] = 'active'
                height = fundingUtxo.get('height')
                if height and height > 0:
                    script['funding_confirmed_height'] = height
                    script['funding_confirmed_at'] = (
                        dt.datetime.utcnow().isoformat() + 'Z')
        else:
            script['status'] = 'spent'
        newStatus = script.get('status')
        if newStatus != oldStatus:
            self.saveScripts()
            if self.scriptStatusCallback is not None:
                self.scriptStatusCallback(p2shAddress, oldStatus, newStatus)

    def preSend(self) -> bool:
        self.stats = {'status': 'not connected'}
        self.divisibility = self.divisibility or 8
        self.banner = 'not connected'
        self.transactionHistory = self.transactionHistory or []
        self.unspentCurrency = self.unspentCurrency or []
        self.unspentAssets = self.unspentAssets or []
        self.currency = self.currency or 0
        self.balance = self.balance or 0
        return False

    def get(self, *args, **kwargs):
        ''' gets data from the blockchain, saves to attributes '''
        try:
            self.maybeConnect()
            if self.banner is None or self.banner == 'not connected':
                self.getStats()
                self.getTransactionHistory()
            self.getBalances()
            self.updateBalances()
        except Exception as e:
            logging.debug('unable to get balances', e)

    def getStats(self):
        if not self.electrumx.ensureConnected():
            return
        self.stats = self.electrumx.api.getStats()
        self.divisibility = Wallet.openSafely(self.stats, 'divisions', 8)
        self.divisibility = self.divisibility if self.divisibility is not None else 8
        self.banner = self.electrumx.api.getBanner()

    def getTransactionHistory(self):
        if not self.electrumx.ensureConnected():
            return
        self.transactionHistory = self.electrumx.api.getTransactionHistory(
            scripthash=self.scripthash)

    def getBalances(self):
        if not self.electrumx.ensureConnected():
            return
        self.balances = self.electrumx.api.getBalances(scripthash=self.scripthash)
        self.currency = Balance.fromBalances('EVR', self.balances or {})
        self.balance = Balance.fromBalances('SATORI', self.balances or {})
        if self.balanceUpdatedCallback is not None:
            self.balanceUpdatedCallback(kind='wallet', evr=self.currency, satori=self.balance)

    def getReadyToSend(self, balance: bool = False, save: bool = True):
        if not self.electrumx.ensureConnected():
            return
        try:
            self.maybeConnect()
            # Ensure divisibility is set from blockchain stats before sending
            if self.divisibility == 0:
                self.getStats()
            if balance:
                self.getBalances()
            self.getUnspents()
            self.getUnspentTransactions(threaded=False)
            self.getUnspentSignatures()
            if save:
                self.saveCache()
        except Exception as e:
            logging.debug('unable to get reaedy to send', e)

    def getReadyToSendSimplified(self, balance: bool = False, save: bool = True):
        '''
        Fast alternative to getReadyToSend for P2PKH wallets. Skips fetching
        transactions from ElectrumX entirely by computing scriptPubKeys
        locally from the wallet's public key. This avoids the bottleneck
        where getUnspentTransactions fetches a raw transaction (with 0.34s
        throttle) for EVERY UTXO tx_hash including all SATORI asset UTXOs.

        For P2PKH the scriptPubKey is deterministic:
            OP_DUP OP_HASH160 <hash160(pubkey)> OP_EQUALVERIFY OP_CHECKSIG
        For asset UTXOs, _compileInputs() constructs the full scriptPubKey
        (including OP_EVR_ASSET data) from the UTXO value at signing time.

        NOTE: This will NOT work for P2SH addresses where the redeem script
        must be fetched from the chain. Use getReadyToSend for P2SH wallets.
        '''
        if not self.electrumx.ensureConnected():
            return
        try:
            self.maybeConnect()
            if self.divisibility == 0:
                self.getStats()
            if balance:
                self.getBalances()
            self.getUnspents()
            self.computeUnspentScriptPubKeys()
            if save:
                self.saveCache()
        except Exception as e:
            logging.debug('unable to get ready to send (simplified)', e)

    def computeUnspentScriptPubKeys(self):
        '''
        Computes the scriptPubKey for currency UTXOs directly from the
        wallet's public key, without fetching transactions from ElectrumX.
        For standard P2PKH addresses the scriptPubKey is deterministic:
            OP_DUP OP_HASH160 <hash160(pubkey)> OP_EQUALVERIFY OP_CHECKSIG
        This is byte-for-byte identical to what's on-chain.

        Asset UTXOs are left without scriptPubKey because their on-chain
        scriptPubKey includes OP_EVR_ASSET data. _compileInputs() has a
        fallback that correctly constructs the full asset scriptPubKey from
        the UTXO's value and the wallet's public key at signing time.
        '''
        from evrmore.core import Hash160
        from evrmore.core.script import (
            CScript, OP_DUP, OP_HASH160, OP_EQUALVERIFY, OP_CHECKSIG)
        scriptPubKeyHex = CScript([
            OP_DUP, OP_HASH160,
            Hash160(self.publicKeyBytes),
            OP_EQUALVERIFY, OP_CHECKSIG,
        ]).hex()
        for uc in (self.unspentCurrency or []):
            if uc.get('scriptPubKey') is None:
                uc['scriptPubKey'] = scriptPubKeyHex

    def getUnspents(self):
        if not self.electrumx.ensureConnected():
            return
        #import traceback
        #traceback.print_stack()
        #self.electrumx.ensureConnected()
        self.unspentCurrency = self.electrumx.api.getUnspentCurrency(scripthash=self.scripthash) or []
        self.unspentCurrency = [
            x for x in self.unspentCurrency
            if x.get('asset') == None]
        self.unspentAssets = []
        if 'SATORI' in self.watchAssets:
            # never used:
            #self.balanceOnChain = self.electrumx.api.getBalance(scripthash=self.scripthash)
            #logging.debug('self.balanceOnChain', self.balanceOnChain)
            # mempool sends all unspent transactions in currency and assets so we have to filter them here:
            self.unspentAssets = self.electrumx.api.getUnspentAssets(scripthash=self.scripthash) or []
            self.unspentAssets = [
                x for x in self.unspentAssets
                if x.get('asset') != None]
            logging.debug('self.unspentAssets', self.unspentAssets)

    def deriveBalanceFromUnspents(self):
        ''' though I like the one source of truth we don't do this anymore '''
        for x in self.unspentCurrency:
            Wallet.openSafely(x, 'value', 0)
        self.currency = sum([
            x.get('value', 0)
            for x in self.unspentCurrency
            if x.get('asset') == None])
        self.currencyAmount = TxUtils.asAmount(self.currency or 0, 8)
        if 'SATORI' in self.watchAssets:
            self.balance = sum([
                x.get('value', 0)
                for x in self.unspentAssets
                if (x.get('name', x.get('asset')) == 'SATORI' and
                    x.get('value') > 0)])
            logging.debug('self.balance', self.balance)
            self.balance.amount = TxUtils.asAmount(
                self.balance or 0,
                self.divisibility)

    def getUnspentTransactions(self, threaded: bool = True, then: callable = None) -> bool:

        def run():
            transactionIds = {tx.txid for tx in self.transactions}
            txids = [uc['tx_hash'] for uc in self.unspentCurrency] + [ua['tx_hash'] for ua in self.unspentAssets]
            for txid in txids:
                if txid not in transactionIds:
                    raw = self.electrumx.api.getTransaction(txid)
                    logging.debug('pulling transaction:', txid, color='blue')
                    if raw is not None:
                        self.transactions.append(TransactionStruct(
                            raw=raw,
                            vinVoutsTxids=[
                                vin.get('txid', '')
                                for vin in raw.get('vin', {})
                                if vin.get('txid', '') != '']))
            if callable(then):
                then()

        if not self.electrumx.ensureConnected():
            return False
        if threaded:
            self.getUnspentTransactionsThread = threading.Thread(
                target=run, daemon=True)
            self.getUnspentTransactionsThread.start()
        else:
            run()
            if callable(then):
                then()
        return True

    ### Functions ##############################################################

    def appendTransaction(self, txid):
        if not self.electrumx.ensureConnected():
            return
        #self.electrumx.ensureConnected()
        if txid not in self._transactions.keys():
            raw = self.electrumx.api.getTransaction(txid)
            if raw is not None:
                if self.pullFullTransactions:
                    txs = []
                    txIds = []
                    for vin in raw.get('vin', {}):
                        txId = vin.get('txid', '')
                        if txId == '':
                            continue
                        txIds.append(txId)
                        txs.append(self.electrumx.api.getTransaction(txId))
                    transaction = TransactionStruct(
                        raw=raw,
                        vinVoutsTxids=txIds,
                        vinVoutsTxs=[t for t in txs if t is not None])
                    self.transactions.append(transaction)
                    self._transactions[txid] = transaction.export()
                    return transaction.export()
                else:
                    txs = []
                    txIds = []
                    for vin in raw.get('vin', {}):
                        txId = vin.get('txid', '')
                        if txId == '':
                            continue
                        txIds.append(txId)
                        # <--- don't get the inputs to the transaction here
                    transaction = TransactionStruct(
                        raw=raw,
                        vinVoutsTxids=txIds)
                    self.transactions.append(transaction)
        else:
            raw, txids, txs = self._transactions.get(txid, ({}, []))
            self.transactions.append(
                TransactionStruct(
                    raw=raw,
                    vinVoutsTxids=txids,
                    vinVoutsTxs=txs))

    def callTransactionHistory(self):
        def getTransactions(transactionHistory: dict) -> list:
            self.transactions = []
            if not isinstance(transactionHistory, list):
                return
            new_transactions = {}  # Collect new transactions here
            for tx in transactionHistory:
                txid = tx.get('tx_hash', '')
                new_tranaction = self.appendTransaction(txid)
                if new_tranaction is not None:
                    new_transactions[txid] = new_tranaction
            # why not save self._transactions to cache? because these are incremental.
            #self.saveCache(new_transactions)

        # self.getTransactionsThread = threading.Thread(
        #    target=getTransactions, args=(self.transactionHistory,), daemon=True)
        # self.getTransactionsThread.start()


    def showStats(self):
        ''' returns a string of stats properly formatted '''
        def invertDivisibility(divisibility: int):
            return (16 + 1) % (divisibility + 8 + 1)

        stats = (self.stats or {})
        divisions = stats.get('divisions', 8)
        circulatingCoins = TxUtils.asAmount(int(stats.get(
            'sats_in_circulation', 100000000000000)))
        # circulatingSats = stats.get(
        #    'sats_in_circulation', 100000000000000) / int('1' + ('0'*invertDivisibility(int(divisions))))
        # headTail = str(circulatingSats).split('.')
        # if headTail[1] == '0' or headTail[1] == '00000000':
        #    circulatingSats = f"{int(headTail[0]):,}"
        # else:
        #    circulatingSats = f"{int(headTail[0]):,}" + '.' + \
        #        f"{headTail[1][0:4]}" + '.' + f"{headTail[1][4:]}"
        return f'''
    Circulating Supply: {circulatingCoins}
    Decimal Points: {divisions}
    Reissuable: {stats.get('reissuable', False)}
    Issuing Transactions: {stats.get('source', {}).get('tx_hash', self.satoriOriginalTxHash)}
    '''

    def registerPayload(
        self,
        asDict: bool = False,
        challenge: str = None,
        vaultInfo:dict = None,
    ) -> Union[str, dict]:
        payload = {
            **self.identity.authPayload(asDict=True, challenge=challenge),
            **system.devicePayload(asDict=True), 
            **(vaultInfo or {})}
        if asDict:
            return payload
        return json.dumps(payload)

    ### Transaction Support ####################################################


    def shouldPullUnspents(self) -> bool:
        return not (
            self.lastBalanceAmount == (self.balance.amount or 0) and
            self.lastCurrencyAmount == (self.currency.amount or 0))

    def updateBalances(self):
        self.lastBalanceAmount = self.balance.amount
        self.lastCurrencyAmount = self.currency.amount


    def getUnspentSignatures(self, force: bool = False) -> bool:
        '''
        we don't need to get the scriptPubKey every time we open the wallet,
        and it requires lots of calls for individual transactions.
        we just need them available when we're creating transactions.
        '''
        if 'SATORI' in self.watchAssets:
            unspents = [
                u for u in self.unspentCurrency + self.unspentAssets
                if 'scriptPubKey' not in u]
        else:
            unspents = [
                u for u in self.unspentCurrency
                if 'scriptPubKey' not in u]
        if not force and len(unspents) == 0:
            # already have them all
            return True

        try:
            # subscription is not necessary for this
            # make sure we're connected
            # if not hasattr(self, 'electrumx') or not self.electrumx.connected():
            #    self.connect()
            # self.get()

            # get transactions, save their scriptPubKey hex to the unspents
            for uc in self.unspentCurrency:
                if uc.get('scriptPubKey', None) is not None:
                    continue
                if len([tx for tx in self.transactions if tx.txid == uc['tx_hash']]) == 0:
                    new_transactions = {}  # Collect new transactions here
                    new_tranaction = self.appendTransaction(uc['tx_hash'])
                    if new_tranaction is not None:
                        new_transactions[uc['tx_hash']] = new_tranaction
                    #self.saveCache(new_transactions)
                tx = [tx for tx in self.transactions if tx.txid == uc['tx_hash']]
                if len(tx) > 0:
                    vout = [vout for vout in tx[0].raw.get(
                        'vout', []) if vout.get('n') == uc['tx_pos']]
                    if len(vout) > 0:
                        scriptPubKey = vout[0].get(
                            'scriptPubKey', {}).get('hex', None)
                        if scriptPubKey is not None:
                            uc['scriptPubKey'] = scriptPubKey
            if 'SATORI' in self.watchAssets:
                for ua in self.unspentAssets:
                    if ua.get('scriptPubKey', None) is not None:
                        continue
                    if len([tx for tx in self.transactions if tx.txid == ua['tx_hash']]) == 0:
                        new_transactions = {}  # Collect new transactions here
                        new_tranaction = self.appendTransaction(ua['tx_hash'])
                        if new_tranaction is not None:
                            new_transactions[ua['tx_hash']] = new_tranaction
                        #self.saveCache(new_transactions)
                    tx = [tx for tx in self.transactions if tx.txid == ua['tx_hash']]
                    if len(tx) > 0:
                        vout = [vout for vout in tx[0].raw.get(
                            'vout', []) if vout.get('n') == ua['tx_pos']]
                        if len(vout) > 0:
                            scriptPubKey = vout[0].get(
                                'scriptPubKey', {}).get('hex', None)
                            if scriptPubKey is not None:
                                ua['scriptPubKey'] = scriptPubKey
        except Exception as e:
            logging.warning(
                'unable to acquire signatures of unspent transactions, maybe unable to send', e, print=True)
            return False
        return True

    def getUnspentsFromHistory(self) -> tuple[list, list]:
        '''
            get unspents from transaction history
            I have to figure out what the VOUTs are myself -
            and I have to split them into lists of currency and Satori outputs
            get history, loop through all transactions, gather all vouts
            loop through all transactions again, remove the vouts that are referenced by vins
            loop through the remaining vouts which are the unspent vouts
            and throw the ones away that are assets but not satori,
            and save the others as currency or satori outputs
            self.transactionHistory structure: [{
                "height": 215008,
                "tx_hash": "f3e1bf48975b8d6060a9de8884296abb80be618dc00ae3cb2f6cee3085e09403"
            }]
            unspents structure: [{
                "tx_pos": 0,
                "value": 45318048,
                "tx_hash": "9f2c45a12db0144909b5db269415f7319179105982ac70ed80d76ea79d923ebf",
                "height": 437146 # optional
            }]
        '''

        for txRef in self.transactionHistory:
            txRef['tx_hash']

    def _checkSatoriValue(self, output: 'CMutableTxOut', amount: float=None) -> bool:
        '''
        returns true if the output is a satori output of amount or self.mundoFee
        '''

    def _gatherReservedCurrencyUnspent(self, exactSats: int = 0):
        unspentCurrency = [
            x for x in self.unspentCurrency if x.get('value') == exactSats]
        if len(unspentCurrency) == 0:
            return None
        return unspentCurrency[0]

    def _gatherOneCurrencyUnspent(self, atleastSats: int = 0, claimed: dict = None) -> tuple:
        claimed = claimed or {}
        for unspentCurrency in self.unspentCurrency:
            if (
                unspentCurrency.get('value') >= atleastSats and
                unspentCurrency.get('tx_hash') not in claimed.keys()
            ):
                return unspentCurrency, unspentCurrency.get('value'), len(self.unspentCurrency)
        return None, 0, 0

    def _gatherCurrencyUnspents(
        self,
        sats: int = 0,
        inputCount: int = 0,
        outputCount: int = 0,
        randomly: bool = False,
        feeRate: int = 150000,
        feeOverride: Optional[int] = None,
    ) -> tuple[list, int]:
        unspentCurrency = [
            x for x in (self.unspentCurrency or []) if x.get('value') > 0]
        unspentCurrency = sorted(unspentCurrency, key=lambda x: x['value'])
        haveCurrency = sum([x.get('value') for x in unspentCurrency])
        if (haveCurrency < sats + (feeOverride or self.reserve)):
            raise TransactionFailure(
                'tx: must retain a reserve of currency to cover fees')
        gatheredCurrencySats = 0
        gatheredCurrencyUnspents = []
        encounteredDust = False
        while (
            gatheredCurrencySats < sats + (feeOverride or TxUtils.estimatedFee(
                inputCount=inputCount + len(gatheredCurrencyUnspents),
                outputCount=outputCount,
                feeRate=feeRate))
        ):
            if randomly:
                randomUnspent = unspentCurrency.pop(
                    randrange(len(unspentCurrency)))
                gatheredCurrencyUnspents.append(randomUnspent)
                gatheredCurrencySats += randomUnspent.get('value')
            else:
                try:
                    smallestUnspent = unspentCurrency.pop(0)
                    gatheredCurrencyUnspents.append(smallestUnspent)
                    gatheredCurrencySats += smallestUnspent.get('value')
                except IndexError as _:
                    # this usually happens when people have lots of dust.
                    encounteredDust = True
                    break
        if encounteredDust:
            unspentCurrency = gatheredCurrencyUnspents
            gatheredCurrencySats = 0
            gatheredCurrencyUnspents = []
            while (
                gatheredCurrencySats < sats + (feeOverride or TxUtils.estimatedFee(
                    inputCount=inputCount + len(gatheredCurrencyUnspents),
                    outputCount=outputCount))
            ):
                if randomly:
                    randomUnspent = unspentCurrency.pop(
                        randrange(len(unspentCurrency)))
                    gatheredCurrencyUnspents.append(randomUnspent)
                    gatheredCurrencySats += randomUnspent.get('value')
                else:
                    try:
                        largestUnspent = unspentCurrency.pop()
                        gatheredCurrencyUnspents.append(largestUnspent)
                        gatheredCurrencySats += largestUnspent.get('value')
                    except IndexError as _:
                        # they simply do not have enough currency to send
                        # it might all be dust.
                        # at least we can still try to make the transaction...
                        break
        return (gatheredCurrencyUnspents, gatheredCurrencySats)

    def _gatherSatoriUnspents(
        self,
        sats: int,
        randomly: bool = False
    ) -> tuple[list, int]:
        unspentSatori = [x for x in (self.unspentAssets or []) if x.get(
            'name', x.get('asset')) == 'SATORI' and x.get('value') > 0]
        unspentSatori = sorted(unspentSatori, key=lambda x: x['value'])
        haveSatori = sum([x.get('value') for x in unspentSatori])
        if not (haveSatori >= sats > 0):
            raise TransactionFailure('tx: not enough satori to send')
        # gather satori utxos at random
        gatheredSatoriSats = 0
        gatheredSatoriUnspents = []
        while gatheredSatoriSats < sats:
            if randomly:
                randomUnspent = unspentSatori.pop(
                    randrange(len(unspentSatori)))
                gatheredSatoriUnspents.append(randomUnspent)
                gatheredSatoriSats += randomUnspent.get('value')
            else:
                smallestUnspent = unspentSatori.pop(0)
                gatheredSatoriUnspents.append(smallestUnspent)
                gatheredSatoriSats += smallestUnspent.get('value')
        return (gatheredSatoriUnspents, gatheredSatoriSats)

    def _compileInputs(
        self,
        gatheredCurrencyUnspents: list = None,
        gatheredSatoriUnspents: list = None,
    ) -> tuple[list, list]:
        ''' compile inputs '''
        # see https://github.com/sphericale/python-evrmorelib/blob/master/examples/spend-p2pkh-txout.py

    def _compileSatoriOutputs(self, satsByAddress: dict[str, int] = None) -> list:
        ''' compile satori outputs'''
        # see https://github.com/sphericale/python-evrmorelib/blob/master/examples/spend-p2pkh-txout.py
        # vouts
        # how do I specify an asset output? this doesn't seem right for that:
        #         OP_DUP  OP_HASH160 3d5143a9336eaf44990a0b4249fcb823d70de52c OP_EQUALVERIFY OP_CHECKSIG OP_RVN_ASSET 0c72766e6f075341544f524921 75
        #         OP_DUP  OP_HASH160 3d5143a9336eaf44990a0b4249fcb823d70de52c OP_EQUALVERIFY OP_CHECKSIG 0c(OP_RVN_ASSET) 72766e(rvn) 74(t) 07(length) 5341544f524921(SATORI) 00e1f50500000000(padded little endian hex of 100000000) 75(drop)
        #         OP_DUP  OP_HASH160 3d5143a9336eaf44990a0b4249fcb823d70de52c OP_EQUALVERIFY OP_CHECKSIG 0c(OP_RVN_ASSET) 72766e(rvn) 74(t) 07(length) 5341544f524921(SATORI) 00e1f50500000000(padded little endian hex of 100000000) 75(drop)
        #         OP_DUP  OP_HASH160 3d5143a9336eaf44990a0b4249fcb823d70de52c OP_EQUALVERIFY OP_CHECKSIG 0c(OP_RVN_ASSET) 14(20 bytes length of asset information) 657672(evr) 74(t) 07(length of asset name) 5341544f524921(SATORI is asset name) 00e1f50500000000(padded little endian hex of 100000000) 75(drop)
        #         OP_DUP  OP_HASH160 3d5143a9336eaf44990a0b4249fcb823d70de52c OP_EQUALVERIFY OP_CHECKSIG 0c1465767274075341544f52492100e1f5050000000075
        # CScript([OP_DUP, OP_HASH160, Hash160(self.publicKey.encode()), OP_EQUALVERIFY, OP_CHECKSIG ])
        # CScript([OP_DUP, OP_HASH160, Hash160(self.publicKey.encode()), OP_EQUALVERIFY, OP_CHECKSIG OP_EVR_ASSET 0c ])
        #
        # for asset transfer...? perfect?
        #   >>> Hash160(CRavencoinAddress(address).to_scriptPubKey())
        #   b'\xc2\x0e\xdf\x8cG\xd7\x8d\xac\x052\x03\xddC<0\xdd\x00\x91\xd9\x19'
        #   >>> Hash160(CRavencoinAddress(address))
        #   b'!\x8d"6\xcf\xe8\xf6W4\x830\x85Y\x06\x01J\x82\xc4\x87p' <- looks like what we get with self.pubkey.encode()
        # https://ravencoin.org/assets/
        # https://rvn.cryptoscope.io/api/getrawtransaction/?txid=bae95f349f15effe42e75134ee7f4560f53462ddc19c47efdd03f85ef4ab8f40&decode=1
        #
        # todo: you could generalize this to send any asset. but not necessary.

    def _compileCurrencyOutputs(self, satsByAddress: dict[str, int]=None, currencySats: int=None, address: str=None) -> list['CMutableTxOut']:
        ''' compile currency outputs'''

    def _compileSatoriChangeOutput(
        self,
        satoriSats: int = 0,
        gatheredSatoriSats: int = 0,
    ) -> 'CMutableTxOut':
        ''' compile satori change output '''

    def _compileCurrencyChangeOutput(
        self,
        currencySats: int = 0,
        gatheredCurrencySats: int = 0,
        inputCount: int = 0,
        outputCount: int = 0,
        fee: int = 0,
        scriptPubKey: 'CScript' = None,
        returnSats: bool = False,
    ) -> Union['CMutableTxOut', None, tuple['CMutableTxOut', int]]:
        ''' compile currency change output '''

    def _compileMemoOutput(self, memo: str) -> 'CMutableTxOut':
        '''
        compile op_return memo output
        for example:
            {"value":0,
            "n":0,
            "scriptPubKey":{"asm":"OP_RETURN 1869440365",
            "hex":"6a046d656d6f",
            "type":"nulldata"},
            "valueSat":0},
        '''

    def _createTransaction(self, txins: list, txinScripts: list, txouts: list) -> 'CMutableTransaction':
        ''' create transaction '''

    def _createPartialOriginatorSimple(self, txins: list, txinScripts: list, txouts: list) -> 'CMutableTransaction':
        ''' originate partial '''

    def _createPartialCompleterSimple(self, txins: list, txinScripts: list, tx: 'CMutableTransaction') -> 'CMutableTransaction':
        ''' complete partial '''

    def _txToHex(self, tx: 'CMutableTransaction') -> str:
        ''' serialize '''

    def _serialize(self, tx: 'CMutableTransaction') -> bytes:
        ''' serialize '''

    def _deserialize(self, serialTx: bytes) -> 'CMutableTransaction':
        ''' serialize '''

    def broadcast(self, txHex: str) -> str:
        """Broadcast a signed tx. Raises TransactionFailure if the node rejects
        it (ElectrumX returns a {'code', 'message'} error dict). Returns the
        plain txid string on success.
        """
        result = self.electrumx.api.broadcast(txHex)
        if isinstance(result, dict) and result.get('code') is not None:
            raise TransactionFailure(
                f'broadcast rejected: {result.get("message", result)}')
        if isinstance(result, (list, tuple)):
            result = result[0] if result else ''
        if result is None:
            raise TransactionFailure('broadcast returned no result')
        return str(result)

    ### Transactions ###########################################################

    ### p2sh - lock ########################################################################

    def produceSimpleTime(
        self,
        scripts: dict[str, dict],
        memo: str=None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
    ) -> tuple[str, str, dict[str, dict]]:
        ''' creates a transaction with multiple SATORI asset recipients '''
        if len(scripts) == 0 or len(scripts) > 1000:
            raise TransactionFailure('too many or too few scripts')
        satoriSats = 0
        inferredVout = 0
        for date, payload in scripts.items():
            amount = payload['amount']
            address = payload['p2sh_address']
            size = len(payload['redeem_script'])
            if (
                amount <= 0 or
                # not TxUtils.isAmountDivisibilityValid(
                #    amount=amount,
                #    divisibility=self.divisibility) or
                not Validate.address(address, self.symbol) or
                size < 1 or size > 50000
            ):
                logging.info(
                    'amount', amount,
                    'divisibility', self.divisibility,
                    'address', address,
                    'address valid:', Validate.address(address, self.symbol),
                    'size', size,
                    'TxUtils.isAmountDivisibilityValid(amount=amount,divisibility=self.divisibility)', TxUtils.isAmountDivisibilityValid(amount=amount, divisibility=self.divisibility), color='green')
                raise TransactionFailure('produceSimpleTime bad params')
            scripts[date]['funding_vout'] = inferredVout
            scripts[date]['satori_sats'] = TxUtils.roundSatsDownToDivisibility(
                sats=TxUtils.asSats(amount),
                divisibility=self.divisibility)
            satoriSats += scripts[date]['satori_sats']
            inferredVout += 1
        memoCount = 0
        if memo is not None:
            memoCount = 1
        (
            gatheredSatoriUnspents,
            gatheredSatoriSats) = self._gatherSatoriUnspents(satoriSats)
        (
            gatheredCurrencyUnspents,
            gatheredCurrencySats) = self._gatherCurrencyUnspents(
                feeOverride=feeOverride,
                inputCount=len(gatheredSatoriUnspents),
                outputCount=len(scripts) + 2 + memoCount)
        txins, txinScripts = self._compileInputs(
            gatheredCurrencyUnspents=gatheredCurrencyUnspents,
            gatheredSatoriUnspents=gatheredSatoriUnspents)
        satsByAddress = {payload['p2sh_address']: payload['satori_sats'] for payload in scripts.values()}
        satoriOuts = self._compileSatoriOutputs(satsByAddress)
        satoriChangeOut = self._compileSatoriChangeOutput(
            satoriSats=satoriSats,
            gatheredSatoriSats=gatheredSatoriSats)
        fee = feeOverride or TxUtils.estimatedFee(
            inputCount=len(txins),
            outputCount=len(satsByAddress) + 2 + memoCount)  # satoriChange, currencyChange, memo
        currencyChangeOut = self._compileCurrencyChangeOutput(
            gatheredCurrencySats=gatheredCurrencySats,
            fee=fee)
        memoOut = None
        if memo is not None:
            memoOut = self._compileMemoOutput(memo)
        tx = self._createTransaction(
            txins=txins,
            txinScripts=txinScripts,
            txouts=satoriOuts + [
                x for x in [satoriChangeOut, currencyChangeOut, memoOut]
                if x is not None])
        requiredFee = TxUtils.getTxFee(self._txToHex(tx), TxUtils.feeRate)
        print('estimated fee:', fee, 'actual fee:', requiredFee)
        if requiredFee * 0.99 < fee < requiredFee * 1.25:
            if broadcast:
                funding_txid = self.broadcast(self._txToHex(tx))
                for date, payload in scripts.items():
                    scripts[date]['funding_txid'] = funding_txid
                    self.saveScript(payload)
                return self._txToHex(tx), funding_txid, scripts
            return self._txToHex(tx), '', scripts
        return self.produceSimpleTime(
            scripts=scripts,
            memo=memo,
            broadcast=broadcast,
            feeOverride=requiredFee)

    def produceSimpleTimeCurrency(
        self,
        scripts: dict[str, dict],
        memo: str=None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
    ) -> tuple[str, str, dict[str, dict]]:
        ''' creates a transaction with multiple currency recipients '''
        if len(scripts) == 0 or len(scripts) > 500:
            raise TransactionFailure('too many or too few scripts')
        currencySats = 0
        inferredVout = 0
        for date, payload in scripts.items():
            amount = payload['amount']
            address = payload['p2sh_address']
            size = len(payload['redeem_script'])
            if (
                amount <= 0 or
                # not TxUtils.isAmountDivisibilityValid(
                #    amount=amount,
                #    divisibility=self.divisibility) or
                not Validate.address(address, self.symbol) or
                size < 1 or size > 50000
            ):
                logging.info(
                    'amount', amount,
                    'divisibility', self.divisibility,
                    'address', address,
                    'address valid:', Validate.address(address, self.symbol),
                    'size', size,
                    'TxUtils.isAmountDivisibilityValid(amount=amount,divisibility=self.divisibility)', TxUtils.isAmountDivisibilityValid(amount=amount, divisibility=self.divisibility), color='green')
                raise TransactionFailure('produceSimpleTimeCurrency bad params')
            scripts[date]['funding_vout'] = inferredVout
            scripts[date]['currency_sats'] = TxUtils.roundSatsDownToDivisibility(
                sats=TxUtils.asSats(amount),
                divisibility=self.divisibility)
            currencySats += scripts[date]['currency_sats']
            inferredVout += 1
        memoCount = 0
        if memo is not None:
            memoCount = 1
        (
            gatheredCurrencyUnspents,
            gatheredCurrencySats) = self._gatherCurrencyUnspents(
                feeOverride=feeOverride,
                sats=currencySats,
                inputCount=1,
                outputCount=len(scripts) + 1 + memoCount)
        txins, txinScripts = self._compileInputs(
            gatheredCurrencyUnspents=gatheredCurrencyUnspents)
        satsByAddress = {payload['p2sh_address']: payload['currency_sats'] for payload in scripts.values()}
        currencyOuts = self._compileCurrencyOutputs(satsByAddress)
        fee = feeOverride or TxUtils.estimatedFee(
            inputCount=len(txins),
            outputCount=len(satsByAddress) + 1 + memoCount)  # currencyChange, memo
        currencyChangeOut = self._compileCurrencyChangeOutput(
            currencySats=currencySats,
            gatheredCurrencySats=gatheredCurrencySats,
            fee=fee)
        memoOut = None
        if memo is not None:
            memoOut = self._compileMemoOutput(memo)
        tx = self._createTransaction(
            txins=txins,
            txinScripts=txinScripts,
            txouts=currencyOuts + [
                x for x in [currencyChangeOut, memoOut]
                if x is not None])
        requiredFee = TxUtils.getTxFee(self._txToHex(tx), TxUtils.feeRate)
        print('estimated fee:', fee, 'actual fee:', requiredFee)
        if requiredFee * 0.99 < fee < requiredFee * 1.25:
            if broadcast:
                funding_txid = self.broadcast(self._txToHex(tx))
                for date, payload in scripts.items():
                    scripts[date]['funding_txid'] = funding_txid
                    self.saveScript(payload)
                return self._txToHex(tx), funding_txid, scripts
            return self._txToHex(tx), '', scripts
        return self.produceSimpleTimeCurrency(
            scripts=scripts,
            memo=memo,
            broadcast=broadcast,
            feeOverride=requiredFee)

    def produceMultiTimeMultisig(
        self,
        scripts: dict[str, dict],
        memo: str=None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
    ) -> tuple[str, str, dict[str, dict]]:
        ''' creates a transaction with multiple SATORI asset recipients '''
        if len(scripts) == 0 or len(scripts) > 1000:
            raise TransactionFailure('too many or too few scripts')
        satoriSats = 0
        inferredVout = 0
        for date, payload in scripts.items():
            amount = payload['amount']
            address = payload['p2sh_address']
            size = len(payload['redeem_script'])
            if (
                amount <= 0 or
                # not TxUtils.isAmountDivisibilityValid(
                #    amount=amount,
                #    divisibility=self.divisibility) or
                not Validate.address(address, self.symbol) or
                size < 1 or size > 50000
            ):
                logging.info(
                    'amount', amount,
                    'divisibility', self.divisibility,
                    'address', address,
                    'address valid:', Validate.address(address, self.symbol),
                    'size', size,
                    'TxUtils.isAmountDivisibilityValid(amount=amount,divisibility=self.divisibility)', TxUtils.isAmountDivisibilityValid(amount=amount, divisibility=self.divisibility), color='green')
                raise TransactionFailure('produceMultiTimeMultisig bad params')
            scripts[date]['funding_vout'] = inferredVout
            scripts[date]['satori_sats'] = TxUtils.roundSatsDownToDivisibility(
                sats=TxUtils.asSats(amount),
                divisibility=self.divisibility)
            satoriSats += scripts[date]['satori_sats']
            inferredVout += 1
        memoCount = 0
        if memo is not None:
            memoCount = 1
        (
            gatheredSatoriUnspents,
            gatheredSatoriSats) = self._gatherSatoriUnspents(satoriSats)
        (
            gatheredCurrencyUnspents,
            gatheredCurrencySats) = self._gatherCurrencyUnspents(
                feeOverride=feeOverride,
                inputCount=len(gatheredSatoriUnspents),
                outputCount=len(scripts) + 2 + memoCount)
        txins, txinScripts = self._compileInputs(
            gatheredCurrencyUnspents=gatheredCurrencyUnspents,
            gatheredSatoriUnspents=gatheredSatoriUnspents)
        satsByAddress = {payload['p2sh_address']: payload['satori_sats'] for payload in scripts.values()}
        satoriOuts = self._compileSatoriOutputs(satsByAddress)
        satoriChangeOut = self._compileSatoriChangeOutput(
            satoriSats=satoriSats,
            gatheredSatoriSats=gatheredSatoriSats)
        fee = feeOverride or TxUtils.estimatedFee(
            inputCount=len(txins),
            outputCount=len(satsByAddress) + 2 + memoCount)  # currencyChange, memo
        currencyChangeOut = self._compileCurrencyChangeOutput(
            gatheredCurrencySats=gatheredCurrencySats,
            fee=fee)
        memoOut = None
        if memo is not None:
            memoOut = self._compileMemoOutput(memo)
        tx = self._createTransaction(
            txins=txins,
            txinScripts=txinScripts,
            txouts=satoriOuts + [
                x for x in [satoriChangeOut, currencyChangeOut, memoOut]
                if x is not None])
        requiredFee = TxUtils.getTxFee(self._txToHex(tx), TxUtils.feeRate)
        print('estimated fee:', fee, 'actual fee:', requiredFee)
        if requiredFee * 0.99 < fee < requiredFee * 1.25:
            if broadcast:
                funding_txid = self.broadcast(self._txToHex(tx))
                for date, payload in scripts.items():
                    scripts[date]['funding_txid'] = funding_txid
                    self.saveScript(payload)
                return self._txToHex(tx), funding_txid, scripts
            return self._txToHex(tx), '', scripts
        return self.produceMultiTimeMultisig(
            scripts=scripts,
            memo=memo,
            broadcast=broadcast,
            feeOverride=requiredFee)

    def produceMultiTimeMultisigCurrency(
        self,
        scripts: dict[str, dict],
        memo: str=None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
    ) -> tuple[str, str, dict[str, dict]]:
        ''' creates a transaction with multiple currency recipients '''
        if len(scripts) == 0 or len(scripts) > 500:
            raise TransactionFailure('too many or too few scripts')
        currencySats = 0
        inferredVout = 0
        for date, payload in scripts.items():
            amount = payload['amount']
            address = payload['p2sh_address']
            size = len(payload['redeem_script'])
            if (
                amount <= 0 or
                # not TxUtils.isAmountDivisibilityValid(
                #    amount=amount,
                #    divisibility=self.divisibility) or
                not Validate.address(address, self.symbol) or
                size < 1 or size > 50000
            ):
                logging.info(
                    'amount', amount,
                    'divisibility', self.divisibility,
                    'address', address,
                    'address valid:', Validate.address(address, self.symbol),
                    'size', size,
                    'TxUtils.isAmountDivisibilityValid(amount=amount,divisibility=self.divisibility)', TxUtils.isAmountDivisibilityValid(amount=amount, divisibility=self.divisibility), color='green')
                raise TransactionFailure('produceMultiTimeMultisigCurrency bad params')
            scripts[date]['funding_vout'] = inferredVout
            scripts[date]['currency_sats'] = TxUtils.roundSatsDownToDivisibility(
                sats=TxUtils.asSats(amount),
                divisibility=self.divisibility)
            currencySats += scripts[date]['currency_sats']
            inferredVout += 1
        memoCount = 0
        if memo is not None:
            memoCount = 1
        (
            gatheredCurrencyUnspents,
            gatheredCurrencySats) = self._gatherCurrencyUnspents(
                feeOverride=feeOverride,
                sats=currencySats,
                inputCount=1,
                outputCount=len(scripts) + 1 + memoCount)
        txins, txinScripts = self._compileInputs(
            gatheredCurrencyUnspents=gatheredCurrencyUnspents)
        satsByAddress = {payload['p2sh_address']: payload['currency_sats'] for payload in scripts.values()}
        currencyOuts = self._compileCurrencyOutputs(satsByAddress)
        fee = feeOverride or TxUtils.estimatedFee(
            inputCount=len(txins),
            outputCount=len(satsByAddress) + 1 + memoCount)  # currencyChange, memo
        currencyChangeOut = self._compileCurrencyChangeOutput(
            currencySats=currencySats,
            gatheredCurrencySats=gatheredCurrencySats,
            fee=fee)
        memoOut = None
        if memo is not None:
            memoOut = self._compileMemoOutput(memo)
        tx = self._createTransaction(
            txins=txins,
            txinScripts=txinScripts,
            txouts=currencyOuts + [
                x for x in [currencyChangeOut, memoOut]
                if x is not None])
        requiredFee = TxUtils.getTxFee(self._txToHex(tx), TxUtils.feeRate)
        print('estimated fee:', fee, 'actual fee:', requiredFee)
        if requiredFee * 0.99 < fee < requiredFee * 1.25:
            if broadcast:
                funding_txid = self.broadcast(self._txToHex(tx))
                for date, payload in scripts.items():
                    scripts[date]['funding_txid'] = funding_txid
                    self.saveScript(payload)
                return self._txToHex(tx), funding_txid, scripts
            return self._txToHex(tx), '', scripts
        return self.produceMultiTimeMultisigCurrency(
            scripts=scripts,
            memo=memo,
            broadcast=broadcast,
            feeOverride=requiredFee)


    ### p2sh - unlock ########################################################################

    def simpleTimeTransaction(
        self,
        address: str,
        lockedAmount: float,
        memo: str=None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
        fundingTxId: str = None,
        fundingVout: int = None,
        redeemScript: bytes = None, #'CScript'
        timedRelease: bool = True,
        date: Optional[dt.datetime] = None,
    ) -> tuple[str, dict[str, int]]:
        ''' claim locked tokens '''
        from satorilib.wallet.evrmore.scripts.mining import unlock
        if (
            lockedAmount <= 0 or
            # not TxUtils.isAmountDivisibilityValid(
            #    amount=amount,
            #    divisibility=self.divisibility) or
            not Validate.address(address, self.symbol)
        ):
            raise TransactionFailure('simpleTimeTransaction bad params')
        redeemParams = partial(
            unlock.simpleTime,
            timedRelease=timedRelease)
        satoriSats = TxUtils.roundSatsDownToDivisibility(
            sats=TxUtils.asSats(lockedAmount),
            divisibility=self.divisibility)
        fee = feeOverride or TxUtils.defaultFee
        (
            gatheredCurrencyUnspents,
            gatheredCurrencySats) = self._gatherCurrencyUnspents(
                feeOverride=fee)
        txins, txinScripts = self._compileInputs(
            gatheredCurrencyUnspents=gatheredCurrencyUnspents,
            )
        currencyChangeOut = self._compileCurrencyChangeOutput(
            gatheredCurrencySats=gatheredCurrencySats,
            fee=fee)
        memoOut = self._compileMemoOutput(memo)
        tx = self._compileClaimOnP2SH(
            redeemScript=redeemScript,
            redeemParams=redeemParams,
            address=address,
            satoriSats=satoriSats,
            feeOverride=fee,
            fundingTxId=fundingTxId,
            fundingVout=fundingVout,
            date=date,
            extraVins=txins,
            extraVinsTxinScripts=txinScripts,
            extraVouts=([currencyChangeOut] if currencyChangeOut else []) + ([memoOut] if memoOut else []))
        requiredFee = TxUtils.getTxFee(self._txToHex(tx), TxUtils.feeRate)
        print('estimated fee:', fee, 'actual fee:', requiredFee)
        if requiredFee * 0.99 < fee < requiredFee * 1.25:
            if broadcast:
                return self.broadcast(self._txToHex(tx))
            return tx.serialize().hex()
        return self.simpleTimeTransaction(
            address=address,
            lockedAmount=lockedAmount,
            memo=memo,
            broadcast=broadcast,
            fundingTxId=fundingTxId,
            fundingVout=fundingVout,
            redeemScript=redeemScript,
            timedRelease=timedRelease,
            date=date,
            feeOverride=requiredFee)

    def simpleTimeCurrencyTransaction(
        self,
        address: str,
        lockedAmount: float,
        memo: str=None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
        fundingTxId: str = None,
        fundingVout: int = None,
        redeemScript: bytes = None, #'CScript'
        timedRelease: bool = True,
        date: Optional[dt.datetime] = None,
    ) -> tuple[str, dict[str, int]]:
        ''' claim locked tokens '''
        from satorilib.wallet.evrmore.scripts.mining import unlock
        if (
            lockedAmount <= 0 or
            # not TxUtils.isAmountDivisibilityValid(
            #    amount=amount,
            #    divisibility=self.divisibility) or
            not Validate.address(address, self.symbol)
        ):
            raise TransactionFailure('SimpleTimeReleaseCurrencyTransaction bad params')
        redeemParams = partial(
            unlock.simpleTime,
            timedRelease=timedRelease)
        currencySats = TxUtils.roundSatsDownToDivisibility(
            sats=TxUtils.asSats(lockedAmount),
            divisibility=self.divisibility)
        fee = feeOverride or TxUtils.defaultFee
        memoOut = self._compileMemoOutput(memo)
        tx = self._compileClaimOnP2SH(
            redeemScript=redeemScript,
            redeemParams=redeemParams,
            address=address,
            currencySats=currencySats,
            feeOverride=fee,
            fundingTxId=fundingTxId,
            fundingVout=fundingVout,
            extraVouts=[memoOut] if memoOut else [],
            date=date)
        requiredFee = TxUtils.getTxFee(self._txToHex(tx), TxUtils.feeRate)
        print('estimated fee:', fee, 'actual fee:', requiredFee)
        if requiredFee * 0.99 < fee < requiredFee * 1.25:
            if broadcast:
                return self.broadcast(self._txToHex(tx))
            return tx.serialize().hex()
        return self.simpleTimeCurrencyTransaction(
            address=address,
            lockedAmount=lockedAmount,
            memo=memo,
            broadcast=broadcast,
            fundingTxId=fundingTxId,
            fundingVout=fundingVout,
            redeemScript=redeemScript,
            timedRelease=timedRelease,
            date=date,
            feeOverride=requiredFee)

    def multiTimeMultisigTransaction(
        self,
        address: str,
        lockedAmount: float,
        memo: str=None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
        fundingTxId: str = None,
        fundingVout: int = None,
        redeemScript: bytes = None, #'CScript'
        timedRelease: bool = True,
        date: Optional[dt.datetime] = None,
        multisigMap: Optional[dict[str, bytes]] = None, # ordered pubkeys: signatures
    ) -> tuple[str, dict[str, int]]:
        ''' claim locked tokens '''
        if (
            lockedAmount <= 0 or
            # not TxUtils.isAmountDivisibilityValid(
            #    amount=amount,
            #    divisibility=self.divisibility) or
            not Validate.address(address, self.symbol)
        ):
            raise TransactionFailure('SimpleTimeReleaseCurrencyTransaction bad params')
        if multisigMap is None:
            return self.multiTimeNotMultisigTransaction(
                address=address,
                lockedAmount=lockedAmount,
                memo=memo,
                broadcast=broadcast,
                feeOverride=feeOverride,
                fundingTxId=fundingTxId,
                fundingVout=fundingVout,
                redeemScript=redeemScript,
                timedRelease=timedRelease,
                date=date)
        return self.multiTimeMultisigTransactionStart(
                address=address,
                lockedAmount=lockedAmount,
                memo=memo,
                feeOverride=feeOverride,
                fundingTxId=fundingTxId,
                fundingVout=fundingVout,
                date=date)

    def multiTimeNotMultisigTransaction(
        self,
        address: str,
        lockedAmount: float,
        memo: str=None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
        fundingTxId: str = None,
        fundingVout: int = None,
        redeemScript: bytes = None, #'CScript'
        timedRelease: int = 3,
        date: Optional[dt.datetime] = None,
    ) -> tuple[str, dict[str, int]]:
        ''' claim locked tokens '''
        from satorilib.wallet.evrmore.scripts.mining import unlock
        redeemParams = partial(
            unlock.multiTimeMultisig,
            timedRelease=timedRelease)
        satoriSats = TxUtils.roundSatsDownToDivisibility(
            sats=TxUtils.asSats(lockedAmount),
            divisibility=self.divisibility)
        fee = feeOverride or TxUtils.defaultFee

        (
            gatheredCurrencyUnspents,
            gatheredCurrencySats) = self._gatherCurrencyUnspents(
                feeOverride=fee)
        txins, txinScripts = self._compileInputs(
            gatheredCurrencyUnspents=gatheredCurrencyUnspents,
            )
        currencyChangeOut = self._compileCurrencyChangeOutput(
            gatheredCurrencySats=gatheredCurrencySats,
            fee=fee)

        memoOut = self._compileMemoOutput(memo)
        tx = self._compileClaimOnP2SH(
            redeemScript=redeemScript,
            redeemParams=redeemParams,
            address=address,
            satoriSats=satoriSats,
            feeOverride=fee,
            fundingTxId=fundingTxId,
            fundingVout=fundingVout,
            date=date,
            extraVins=txins,
            extraVinsTxinScripts=txinScripts,
            extraVouts=([currencyChangeOut] if currencyChangeOut else []) + ([memoOut] if memoOut else []))
        requiredFee = TxUtils.getTxFee(self._txToHex(tx), TxUtils.feeRate)
        print('estimated fee:', fee, 'actual fee:', requiredFee)
        if requiredFee * 0.99 < fee < requiredFee * 1.25:
            if broadcast:
                return self.broadcast(self._txToHex(tx))
            return tx.serialize().hex()
        return self.multiTimeNotMultisigTransaction(
            address=address,
            lockedAmount=lockedAmount,
            memo=memo,
            broadcast=broadcast,
            fundingTxId=fundingTxId,
            fundingVout=fundingVout,
            redeemScript=redeemScript,
            timedRelease=timedRelease,
            date=date,
            feeOverride=requiredFee)

    def multiTimeMultisigTransactionStart(
        self,
        address: str,
        lockedAmount: float,
        memo: str=None,
        feeOverride: Optional[int] = None,
        fundingTxId: str = None,
        fundingVout: int = None,
        date: Optional[dt.datetime] = None,
    ) -> tuple[str, dict[str, int]]:
        ''' claim locked tokens '''
        satoriSats = TxUtils.roundSatsDownToDivisibility(
            sats=TxUtils.asSats(lockedAmount),
            divisibility=self.divisibility)
        fee = feeOverride or TxUtils.defaultFee*4
        (
            gatheredCurrencyUnspents,
            gatheredCurrencySats) = self._gatherCurrencyUnspents(
                feeOverride=fee)
        txins, txinScripts = self._compileInputs(
            gatheredCurrencyUnspents=gatheredCurrencyUnspents,
            )
        currencyChangeOut = self._compileCurrencyChangeOutput(
            gatheredCurrencySats=gatheredCurrencySats,
            fee=fee)
        memoOut = self._compileMemoOutput(memo)
        tx = self._compileClaimOnP2SHMultiSigStart(
            address=address,
            satoriSats=satoriSats,
            feeOverride=fee,
            fundingTxIds=[fundingTxId],
            fundingVouts=[fundingVout],
            dates=[date],
            extraVins=txins,
            extraVouts=([currencyChangeOut] if currencyChangeOut else []) + ([memoOut] if memoOut else []))
        return tx, txinScripts

    def multiTimeMultisigTransactionMiddle(
        self,
        tx: bytes = None, # CMutableTransaction
        redeemScript: bytes = None, # 'CScript'
        vinIndex: int = 0,
        sighashFlag: int = None,
    ) -> bytes:
        ''' create a signature for the input at vinIndex '''
        return self._compileClaimOnP2SHMultiSigMiddle(
            tx=tx,
            redeemScript=redeemScript,
            vinIndex=vinIndex,
            **({'sighashFlag': sighashFlag} if sighashFlag else {}))

    def multiTimeMultisigTransactionEnd(
        self,
        tx: bytes, # CMutableTransaction
        signatures: list[bytes],
        extraVinsTxinScripts: Optional[list[bytes]] = None,
        broadcast: bool = True,
        feeOverride: int = 93500,
        redeemScript: bytes = None, #'CScript'
        timedRelease: bool = True,
    ) -> tuple[str, dict[str, int]]:
        ''' claim locked tokens '''
        from satorilib.wallet.evrmore.scripts.mining import unlock
        redeemParams = partial(
            unlock.multiTimeMultisig,
            sig=signatures[0],
            sig2=signatures[1],
            sig3=signatures[2],
            sig4=signatures[3],
            sig5=signatures[4],
            timedRelease=timedRelease)
        fee = feeOverride
        tx = self._compileClaimOnP2SHMultiSigEnd(
            tx=tx,
            redeemScript=redeemScript,
            redeemParams=redeemParams,
            extraVinsTxinScripts=extraVinsTxinScripts)
        requiredFee = TxUtils.getTxFee(self._txToHex(tx), TxUtils.feeRate)
        print('estimated fee:', fee, 'actual fee:', requiredFee)
        if fee >= requiredFee:
            if broadcast:
                return self.broadcast(self._txToHex(tx))
            return tx.serialize().hex()
        raise TransactionFailure('fee too low - start over completely')

    def multiTimeMultisigCurrencyTransaction(
        self,
        address: str,
        lockedAmount: float,
        memo: str=None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
        fundingTxId: str = None,
        fundingVout: int = None,
        redeemScript: bytes = None, #'CScript'
        timedRelease: bool = True,
        date: Optional[dt.datetime] = None,
        multisigMap: Optional[dict[str, bytes]] = None, # ordered pubkeys: signatures
    ) -> tuple[str, dict[str, int]]:
        ''' claim locked tokens '''
        if (
            lockedAmount <= 0 or
            # not TxUtils.isAmountDivisibilityValid(
            #    amount=amount,
            #    divisibility=self.divisibility) or
            not Validate.address(address, self.symbol)
        ):
            raise TransactionFailure('SimpleTimeReleaseCurrencyTransaction bad params')
        if multisigMap is None:
            return self.multiTimeNotMultisigCurrencyTransaction(
                address=address,
                lockedAmount=lockedAmount,
                memo=memo,
                broadcast=broadcast,
                feeOverride=feeOverride,
                fundingTxId=fundingTxId,
                fundingVout=fundingVout,
                redeemScript=redeemScript,
                timedRelease=timedRelease,
                date=date)
        return self.multiTimeMultisigCurrencyTransactionStart(
                address=address,
                lockedAmount=lockedAmount,
                memo=memo,
                feeOverride=feeOverride,
                fundingTxId=fundingTxId,
                fundingVout=fundingVout,
                date=date)

    def multiTimeNotMultisigCurrencyTransaction(
        self,
        address: str,
        lockedAmount: float,
        memo: str=None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
        fundingTxId: str = None,
        fundingVout: int = None,
        redeemScript: bytes = None, #'CScript'
        timedRelease: int = 3,
        date: Optional[dt.datetime] = None,
    ) -> tuple[str, dict[str, int]]:
        ''' claim locked tokens '''
        from satorilib.wallet.evrmore.scripts.mining import unlock
        redeemParams = partial(
            unlock.multiTimeMultisig,
            timedRelease=timedRelease)
        currencySats = TxUtils.roundSatsDownToDivisibility(
            sats=TxUtils.asSats(lockedAmount),
            divisibility=self.divisibility)
        fee = feeOverride or TxUtils.defaultFee
        memoOut = self._compileMemoOutput(memo)
        tx = self._compileClaimOnP2SH(
            redeemScript=redeemScript,
            redeemParams=redeemParams,
            address=address,
            currencySats=currencySats,
            feeOverride=fee,
            fundingTxId=fundingTxId,
            fundingVout=fundingVout,
            extraVouts=[memoOut] if memoOut else [],
            date=date)
        requiredFee = TxUtils.getTxFee(self._txToHex(tx), TxUtils.feeRate)
        print('estimated fee:', fee, 'actual fee:', requiredFee)
        if requiredFee * 0.99 < fee < requiredFee * 1.25:
            if broadcast:
                return self.broadcast(self._txToHex(tx))
            return tx.serialize().hex()
        return self.multiTimeNotMultisigCurrencyTransaction(
            address=address,
            lockedAmount=lockedAmount,
            memo=memo,
            broadcast=broadcast,
            fundingTxId=fundingTxId,
            fundingVout=fundingVout,
            redeemScript=redeemScript,
            timedRelease=timedRelease,
            date=date,
            feeOverride=requiredFee)

    def multiTimeMultisigCurrencyTransactionStart(
        self,
        address: str,
        lockedAmount: float,
        memo: str=None,
        feeOverride: Optional[int] = None,
        fundingTxId: str = None,
        fundingVout: int = None,
        date: Optional[dt.datetime] = None,
    ) -> tuple[str, dict[str, int]]:
        ''' claim locked tokens '''
        currencySats = TxUtils.roundSatsDownToDivisibility(
            sats=TxUtils.asSats(lockedAmount),
            divisibility=self.divisibility)
        fee = feeOverride or TxUtils.defaultFee*4
        memoOut = self._compileMemoOutput(memo)
        tx = self._compileClaimOnP2SHMultiSigStart(
            address=address,
            currencySats=currencySats,
            feeOverride=fee,
            fundingTxIds=[fundingTxId],
            fundingVouts=[fundingVout],
            dates=[date],
            extraVouts=[memoOut] if memoOut else [])
        return tx

    def multiTimeMultisigCurrencyTransactionMiddle(
        self,
        tx: bytes = None, # CMutableTransaction
        redeemScript: bytes = None, # 'CScript'
        vinIndex: int = 0,
        sighashFlag: int = None,
    ) -> bytes:
        ''' create a signature for the input at vinIndex '''
        return self._compileClaimOnP2SHMultiSigMiddle(
            tx=tx,
            redeemScript=redeemScript,
            vinIndex=vinIndex,
            **({'sighashFlag': sighashFlag} if sighashFlag else {}))

    def multiTimeMultisigCurrencyTransactionEnd(
        self,
        tx: bytes, # CMutableTransaction
        signatures: list[bytes],
        extraVinsTxinScripts: Optional[list[bytes]] = None,
        broadcast: bool = True,
        feeOverride: int = 93500,
        redeemScript: bytes = None, #'CScript'
        timedRelease: bool = True,
    ) -> tuple[str, dict[str, int]]:
        ''' claim locked tokens '''
        from satorilib.wallet.evrmore.scripts.mining import unlock
        redeemParams = partial(
            unlock.multiTimeMultisig,
            sig=signatures[0],
            sig2=signatures[1],
            sig3=signatures[2],
            sig4=signatures[3],
            sig5=signatures[4],
            timedRelease=timedRelease)
        fee = feeOverride
        tx = self._compileClaimOnP2SHMultiSigEnd(
            tx=tx,
            redeemScript=redeemScript,
            redeemParams=redeemParams,
            extraVinsTxinScripts=extraVinsTxinScripts)
        requiredFee = TxUtils.getTxFee(self._txToHex(tx), TxUtils.feeRate)
        print('estimated fee:', fee, 'actual fee:', requiredFee)
        if fee >= requiredFee:
            if broadcast:
                return self.broadcast(self._txToHex(tx))
            return tx.serialize().hex()
        raise TransactionFailure('fee too low')

    ### p2pkh ########################################################################

    # for server

    def satoriDistribution(
        self,
        amountByAddress: dict[str, float],
        memo: str=None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
    ) -> str:
        ''' creates a transaction with multiple SATORI asset recipients '''
        if len(amountByAddress) == 0 or len(amountByAddress) > 1000:
            raise TransactionFailure('too many or too few recipients')
        satsByAddress: dict[str, int] = {}
        for address, amount in amountByAddress.items():
            if (
                amount <= 0 or
                # not TxUtils.isAmountDivisibilityValid(
                #    amount=amount,
                #    divisibility=self.divisibility) or
                not Validate.address(address, self.symbol)
            ):
                logging.info('amount', amount, 'divisibility', self.divisibility, 'address', address, 'address valid:', Validate.address(address, self.symbol),
                             'TxUtils.isAmountDivisibilityValid(amount=amount,divisibility=self.divisibility)', TxUtils.isAmountDivisibilityValid(amount=amount, divisibility=self.divisibility), color='green')
                raise TransactionFailure('satoriDistribution bad params')
            satsByAddress[address] = TxUtils.roundSatsDownToDivisibility(
                sats=TxUtils.asSats(amount),
                divisibility=self.divisibility)
        memoCount = 0
        if memo is not None:
            memoCount = 1
        satoriSats = sum(satsByAddress.values())
        (
            gatheredSatoriUnspents,
            gatheredSatoriSats) = self._gatherSatoriUnspents(satoriSats)
        (
            gatheredCurrencyUnspents,
            gatheredCurrencySats) = self._gatherCurrencyUnspents(
                feeOverride=feeOverride,
                inputCount=len(gatheredSatoriUnspents),
                outputCount=len(satsByAddress) + 2 + memoCount)
        txins, txinScripts = self._compileInputs(
            gatheredCurrencyUnspents=gatheredCurrencyUnspents,
            gatheredSatoriUnspents=gatheredSatoriUnspents)
        satoriOuts = self._compileSatoriOutputs(satsByAddress)
        satoriChangeOut = self._compileSatoriChangeOutput(
            satoriSats=satoriSats,
            gatheredSatoriSats=gatheredSatoriSats)
        fee = feeOverride or TxUtils.estimatedFee(
            inputCount=len(txins),
            outputCount=len(satsByAddress) + 2 + memoCount)  # satoriChange, currencyChange, memo
        currencyChangeOut = self._compileCurrencyChangeOutput(
            gatheredCurrencySats=gatheredCurrencySats,
            fee=fee)
        memoOut = None
        if memo is not None:
            memoOut = self._compileMemoOutput(memo)
        tx = self._createTransaction(
            txins=txins,
            txinScripts=txinScripts,
            txouts=satoriOuts + [
                x for x in [satoriChangeOut, currencyChangeOut, memoOut]
                if x is not None])
        if broadcast:
            return self.broadcast(self._txToHex(tx))
        return self._txToHex(tx)

    # for neuron
    def currencyTransaction(
        self,
        amount: float,
        address: str,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
    ):
        ''' creates a transaction to just send rvn '''
        if (
            amount <= 0 or
            # not TxUtils.isAmountDivisibilityValid(
            #     amount=amount,
            #     divisibility=8) or
            not Validate.address(address, self.symbol)
        ):
            raise TransactionFailure('bad params for currencyTransaction')
        currencySats = TxUtils.roundSatsDownToDivisibility(
            sats=TxUtils.asSats(amount),
            divisibility=8)
        (
            gatheredCurrencyUnspents,
            gatheredCurrencySats) = self._gatherCurrencyUnspents(
                feeOverride=feeOverride,
                sats=currencySats,
                inputCount=0,
                outputCount=1)
        txins, txinScripts = self._compileInputs(
            gatheredCurrencyUnspents=gatheredCurrencyUnspents)
        currencyOuts = self._compileCurrencyOutputs(address=address, sats=currencySats)
        fee = feeOverride or TxUtils.estimatedFee(
            inputCount=len(txins),
            outputCount=2)
        currencyChangeOut = self._compileCurrencyChangeOutput(
            currencySats=currencySats,
            gatheredCurrencySats=gatheredCurrencySats,
            fee=fee)
        tx = self._createTransaction(
            txins=txins,
            txinScripts=txinScripts,
            txouts=currencyOuts + [
                x for x in [currencyChangeOut]
                if x is not None])
        requiredFee = TxUtils.getTxFee(self._txToHex(tx), TxUtils.feeRate)
        print('estimated fee:', fee, 'actual fee:', requiredFee)
        if requiredFee * 0.99 < fee < requiredFee * 1.25:
            if broadcast:
                return self.broadcast(self._txToHex(tx))
            return self._txToHex(tx)
        return self.currencyTransaction(
            amount=amount,
            address=address,
            feeOverride=requiredFee)

    # for neuron
    def satoriTransaction(self, amount: float, address: str, feeOverride: Optional[int] = None):
        ''' creates a transaction to send satori to one address '''
        if (
            amount <= 0 or
            # not TxUtils.isAmountDivisibilityValid(
            #    amount=amount,
            #    divisibility=self.divisibility) or
            not Validate.address(address, self.symbol)
        ):
            raise TransactionFailure('satoriTransaction bad params')
        satoriSats = TxUtils.roundSatsDownToDivisibility(
            sats=TxUtils.asSats(amount),
            divisibility=self.divisibility)
        (
            gatheredSatoriUnspents,
            gatheredSatoriSats) = self._gatherSatoriUnspents(satoriSats)
        # gather currency in anticipation of fee
        (
            gatheredCurrencyUnspents,
            gatheredCurrencySats) = self._gatherCurrencyUnspents(
                feeOverride=feeOverride,
                inputCount=len(gatheredSatoriUnspents),
                outputCount=3)
        txins, txinScripts = self._compileInputs(
            gatheredCurrencyUnspents=gatheredCurrencyUnspents,
            gatheredSatoriUnspents=gatheredSatoriUnspents)
        satoriOuts = self._compileSatoriOutputs({address: satoriSats})
        satoriChangeOut = self._compileSatoriChangeOutput(
            satoriSats=satoriSats,
            gatheredSatoriSats=gatheredSatoriSats)
        fee = feeOverride or TxUtils.estimatedFee(
            inputCount=len(txins),
            outputCount=3)
        currencyChangeOut = self._compileCurrencyChangeOutput(
            gatheredCurrencySats=gatheredCurrencySats,
            fee=fee)
        tx = self._createTransaction(
            txins=txins,
            txinScripts=txinScripts,
            txouts=satoriOuts + [
                x for x in [satoriChangeOut, currencyChangeOut]
                if x is not None])
        requiredFee = TxUtils.getTxFee(self._txToHex(tx), TxUtils.feeRate)
        print('estimated fee:', fee, 'actual fee:', requiredFee)
        if requiredFee * 0.99 < fee < requiredFee * 1.25:
            return self.broadcast(self._txToHex(tx))
        return self.satoriTransaction(
            amount=amount,
            address=address,
            feeOverride=requiredFee)

    def satoriAndCurrencyTransaction(
        self,
        satoriAmount: float,
        currencyAmount: float,
        address: str,
        feeOverride: Optional[int] = None,
    ):
        ''' creates a transaction to send satori and currency to one address '''
        if (
            satoriAmount <= 0 or
            currencyAmount <= 0 or
            # not TxUtils.isAmountDivisibilityValid(
            #    amount=satoriAmount,
            #    divisibility=self.divisibility) or
            # not TxUtils.isAmountDivisibilityValid(
            #    amount=currencyAmount,
            #    divisibility=8) or
            not Validate.address(address, self.symbol)
        ):
            raise TransactionFailure('satoriAndCurrencyTransaction bad params')
        satoriSats = TxUtils.roundSatsDownToDivisibility(
            sats=TxUtils.asSats(satoriAmount),
            divisibility=self.divisibility)
        currencySats = TxUtils.roundSatsDownToDivisibility(
            sats=TxUtils.asSats(currencyAmount),
            divisibility=8)
        (
            gatheredSatoriUnspents,
            gatheredSatoriSats) = self._gatherSatoriUnspents(satoriSats)
        (
            gatheredCurrencyUnspents,
            gatheredCurrencySats) = self._gatherCurrencyUnspents(
                feeOverride=feeOverride,
                sats=currencySats,
                inputCount=len(gatheredSatoriUnspents),
                outputCount=4)
        txins, txinScripts = self._compileInputs(
            gatheredCurrencyUnspents=gatheredCurrencyUnspents,
            gatheredSatoriUnspents=gatheredSatoriUnspents)
        satoriOuts = self._compileSatoriOutputs({address: satoriSats})
        currencyOuts = self._compileCurrencyOutputs(address=address, sats=currencySats)
        satoriChangeOut = self._compileSatoriChangeOutput(
            satoriSats=satoriSats,
            gatheredSatoriSats=gatheredSatoriSats)
        fee = feeOverride or TxUtils.estimatedFee(
            inputCount=(
                len(gatheredSatoriUnspents) +
                len(gatheredCurrencyUnspents)),
            outputCount=4)
        currencyChangeOut = self._compileCurrencyChangeOutput(
            currencySats=currencySats,
            gatheredCurrencySats=gatheredCurrencySats,
            fee=fee)
        tx = self._createTransaction(
            txins=txins,
            txinScripts=txinScripts,
            txouts=(
                satoriOuts + currencyOuts + [
                    x for x in [satoriChangeOut, currencyChangeOut]
                    if x is not None]))
        requiredFee = TxUtils.getTxFee(self._txToHex(tx), TxUtils.feeRate)
        print('estimated fee:', fee, 'actual fee:', requiredFee)
        if requiredFee * 0.99 < fee < requiredFee * 1.25:
            return self.broadcast(self._txToHex(tx))
        return self.satoriAndCurrencyTransaction(
            satoriAmount=satoriAmount,
            currencyAmount=currencyAmount,
            address=address,
            feeOverride=requiredFee)

    # def satoriOnlyPartial(self, amount: int, address: str, pullFeeFromAmount: bool = False) -> str:
    #    '''
    #    if people do not have a balance of rvn, they can still send satori.
    #    they have to pay the fee in satori, so it's a higher fee, maybe twice
    #    as much on average as a normal transaction. this is because the
    #    variability of the satori price. So this function produces a partial
    #    transaction that can be sent to the server and the rest of the network
    #    to be completed. he who completes the transaction will pay the rvn fee
    #    and collect the satori fee. we will probably broadcast as a json object.
    #
    #    not completed! this generalized version needs to use SIGHASH_SINGLE
    #    which makes the transaction more complex as all inputs need to
    #    correspond to their output. see simple version for more details.
    #
    #    after having completed the simple version, I realized that the easy
    #    solution to the problem of using SIGHASH_SINGLE and needing to issue
    #    change is to simply add an additional input to be assigned to the
    #    change output (a good use of dust, actaully). The only edge case we'd
    #    need to handle is if the user has has no additional utxo to be used as
    #    and input. In that case you'd have to put the process on hold, create a
    #    separate transaction to send the user back to self in order to create
    #    the additional input. That would be a pain, but it is doable, and it
    #    would be a semi-rare case, and it would be a good use of dust, and it
    #    would allow for the general mutli-party-partial-transaction solution.
    #    '''
    #    if (
    #        amount <= 0 or
    #        not TxUtils.isAmountDivisibilityValid(
    #            amount=amount,
    #            divisibility=self.divisibility) or
    #        not Validate.address(address, self.symbol)
    #    ):
    #        raise TransactionFailure('satoriTransaction bad params')
    #    if pullFeeFromAmount:
    #        amount -= self.mundoFee
    #    satoriTotalSats = TxUtils.asSats(amount + self.mundoFee)
    #    satoriSats = TxUtils.asSats(amount)
    #    (
    #        gatheredSatoriUnspents,
    #        gatheredSatoriSats) = self._gatherSatoriUnspents(satoriTotalSats)
    #    txins, txinScripts = self._compileInputs(
    #        gatheredSatoriUnspents=gatheredSatoriUnspents)
    #    # partial transactions need to use Sighash Single so we need to create
    #    # ouputs 1-1 to inputs:
    #    satoriOuts = []
    #    outsAmount = 0
    #    change = 0
    #    for x in gatheredSatoriUnspents:
    #        logging.debug(x.get('value'), color='yellow')
    #        if TxUtils.asAmount(x.get('value'), self.divisibility) + outsAmount < amount:
    #            outAmount = x.get('value')
    #        else:
    #            outAmount = amount - outsAmount
    #            change += x.get('value') - outAmount
    #        outsAmount += outAmount
    #        if outAmount > 0:
    #            satoriOuts.append(
    #                self._compileSatoriOutputs({address: outAmount})[0])
    #    if change - self.mundoFee > 0:
    #        change -= self.mundoFee
    #    if change > 0:
    #        satoriOuts.append(self._compileSatoriOutputs(
    #            {self.address: change})[0])
    #    # needs more work
    #    # satoriOuts = self._compileSatoriOutputs({address: amount})
    #    satoriChangeOut = self._compileSatoriChangeOutput(
    #        satoriSats=satoriSats,
    #        gatheredSatoriSats=gatheredSatoriSats - TxUtils.asSats(self.mundoFee))
    #    tx = self._createPartialOriginator(
    #        txins=txins,
    #        txinScripts=txinScripts,
    #        txouts=satoriOuts + [
    #            x for x in [satoriChangeOut]
    #            if x is not None])
    #    return tx.serialize()
    #
    # def satoriOnlyCompleter(self, serialTx: bytes, address: str) -> str:
    #    '''
    #    a companion function to satoriOnlyTransaction which completes the
    #    transaction add in it's own address for the satori fee and injecting the
    #    necessary rvn inputs to cover the fee. address is the address claim
    #    satori fee address.
    #    '''
    #    tx = self._deserialize(serialTx)
    #    # add rvn fee input
    #    (
    #        gatheredCurrencyUnspents,
    #        gatheredCurrencySats) = self._gatherCurrencyUnspents(
    #            inputCount=len(tx.vin) + 2,  # fee input could potentially be 2
    #            outputCount=len(tx.vout) + 2)  # claim output, change output
    #    txins, txinScripts = self._compileInputs(
    #        gatheredCurrencyUnspents=gatheredCurrencyUnspents)
    #    # add return rvn change output to self
    #    currencyChangeOut = self._compileCurrencyChangeOutput(
    #        gatheredCurrencySats=gatheredCurrencySats,
    #        inputCount=len(tx.vin) + len(txins),
    #        outputCount=len(tx.vout) + 2)
    #    # add satori fee output to self
    #    satoriClaimOut = self._compileSatoriOutputs({address: self.mundoFee})
    #    # sign rvn fee inputs and complete the transaction
    #    tx = self._createTransaction(
    #        tx=tx,
    #        txins=txins,
    #        txinScripts=txinScripts,
    #        txouts=satoriClaimOut + [
    #            x for x in [currencyChangeOut]
    #            if x is not None])
    #    return self.broadcast(self._txToHex(tx))
    #    # return tx  # testing

    def satoriOnlyBridgePartialSimple(
        self,
        amount: int,
        ethAddress: str,
        chain: str='base',
        pullFeeFromAmount: bool = False,
        feeSatsReserved: int = 0,
        completerAddress: str = None,
        changeAddress: str = None,
    ) -> tuple[str, int, str]:
        '''
        if people do not have a balance of rvn, they can still send satori.
        they have to pay the fee in satori. So this function produces a partial
        transaction that can be sent to the server and the rest of the network
        to be completed. he who completes the transaction will pay the rvn fee
        and collect the satori fee. we will probably broadcast as a json object.

        Uses SIGHASH_ANYONECANPAY | SIGHASH_SINGLE so the sender's signature
        locks only the output at the same index as each signed input (the
        sender's change). This allows the completer to add both inputs (for
        fees) and outputs (for fee change).
        '''
        if completerAddress is None or changeAddress is None or feeSatsReserved == 0:
            raise TransactionFailure(
                'Satori Bridge Transaction bad params: need completer details')
        if amount <= 0:
            raise TransactionFailure(
                'Satori Bridge Transaction bad params: amount <= 0')
        if amount > self.maxBridgeAmount:
            raise TransactionFailure(
                'Satori Bridge Transaction bad params: amount > 100')
        if not Validate.ethAddress(ethAddress):
            raise TransactionFailure(
                'Satori Bridge Transaction bad params: eth address')
        if isinstance(amount, Decimal):
            amount = float(amount)
        if self.balance.amount < amount + self.bridgeFee + self.mundoFee:
            raise TransactionFailure(
                f'Satori Bridge Transaction bad params: balance too low to pay for bridgeFee {self.balance.amount} < {amount} + {self.bridgeFee} + {self.mundoFee}')
        if pullFeeFromAmount:
            amount -= self.mundoFee
            amount -= self.bridgeFee
        mundoSatsFee = TxUtils.asSats(self.mundoFee)
        bridgeSatsFee = TxUtils.asSats(self.bridgeFee)
        satoriSats = TxUtils.roundSatsDownToDivisibility(
            sats=TxUtils.asSats(amount),
            divisibility=self.divisibility)
        satoriTotalSats = satoriSats + mundoSatsFee + bridgeSatsFee
        (
            gatheredSatoriUnspents,
            gatheredSatoriSats) = self._gatherSatoriUnspents(satoriTotalSats)
        txins, txinScripts = self._compileInputs(
            gatheredSatoriUnspents=gatheredSatoriUnspents)
        satoriOuts = self._compileSatoriOutputs({self.burnAddress: satoriSats})
        satoriChangeOut = self._compileSatoriChangeOutput(
            satoriSats=satoriSats + mundoSatsFee + bridgeSatsFee,
            gatheredSatoriSats=gatheredSatoriSats)
        # fee out to server
        mundoFeeOut = self._compileSatoriOutputs(
            {completerAddress: mundoSatsFee})[0]
        if mundoFeeOut is None:
            raise TransactionFailure('unable to generate mundo fee')
        # fee out to server
        bridgeFeeOut = self._compileSatoriOutputs(
            {self.bridgeAddress: bridgeSatsFee})[0]
        if bridgeFeeOut is None:
            raise TransactionFailure('unable to generate bridge fee')
        # change out to server
        currencyChangeOut, currencyChange = self._compileCurrencyChangeOutput(
            gatheredCurrencySats=feeSatsReserved,
            inputCount=len(gatheredSatoriUnspents),
            # len([mundoFeeOut, bridgeFeeOut, currencyChangeOut, memoOut]) +
            outputCount=len(satoriOuts) + 4 +
            (1 if satoriChangeOut is not None else 0),
            scriptPubKey=self._generateScriptPubKeyFromAddress(changeAddress),
            returnSats=True)
        if currencyChangeOut is None:
            raise TransactionFailure('unable to generate currency change')
        memoOut = self._compileMemoOutput(f'{chain}:{ethAddress}')
        tx = self._createPartialOriginatorSimple(
            txins=txins,
            txinScripts=txinScripts,
            txouts=satoriOuts + [
                x for x in [satoriChangeOut]
                if x is not None] + [mundoFeeOut, bridgeFeeOut, currencyChangeOut, memoOut])
        reportedFeeSats = feeSatsReserved - currencyChange
        return tx.serialize(), reportedFeeSats, self._txToHex(tx)

    def satoriOnlyPartialSimple(
        self,
        amount: int,
        address: str,
        pullFeeFromAmount: bool = False,
        feeSatsReserved: int = 0,
        feeSats: int = 0,
        satoriFeeAmount: int = 0,
        completerAddress: str = None,
        changeAddress: str = None,
        satoriFeeAddress: str = None,
        **kwargs,
    ) -> tuple[str, int]:
        '''
        if people do not have a balance of evr, they can still send satori.
        they have to pay the fee in satori. So this function produces a partial
        transaction that can be sent to the server and the rest of the network
        to be completed. he who completes the transaction will pay the evr fee
        and collect the satori fee. we will probably broadcast as a json object.

        Uses SIGHASH_ANYONECANPAY | SIGHASH_SINGLE so the sender's signature
        locks only the output at the same index as each signed input (the
        sender's change). This allows the completer to add both inputs (for
        fees) and outputs (for fee change).
        '''
        if completerAddress is None or changeAddress is None or feeSatsReserved == 0:
            raise TransactionFailure('need completer details')
        if (
            amount <= 0 or
            not Validate.address(address, self.symbol)
        ):
            raise TransactionFailure('satoriTransaction bad params')
        # use Mundo-provided SATORI fee, fall back to hardcoded default
        mundoFeeSats = satoriFeeAmount if satoriFeeAmount > 0 else TxUtils.asSats(self.mundoFee)
        mundoFeeFloat = TxUtils.asAmount(mundoFeeSats)
        if pullFeeFromAmount:
            amount -= mundoFeeFloat
        satoriSats = TxUtils.roundSatsDownToDivisibility(
            sats=TxUtils.asSats(amount),
            divisibility=self.divisibility)
        satoriTotalSats = satoriSats + mundoFeeSats
        (
            gatheredSatoriUnspents,
            gatheredSatoriSats) = self._gatherSatoriUnspents(satoriTotalSats)
        txins, txinScripts = self._compileInputs(
            gatheredSatoriUnspents=gatheredSatoriUnspents)
        satoriOuts = self._compileSatoriOutputs({address: satoriSats})
        satoriChangeOut = self._compileSatoriChangeOutput(
            satoriSats=satoriSats,
            gatheredSatoriSats=gatheredSatoriSats - mundoFeeSats)
        # SATORI fee out to Mundo
        feeAddr = satoriFeeAddress or completerAddress
        mundoFeeOut = self._compileSatoriOutputs(
            {feeAddr: mundoFeeSats})[0]
        if mundoFeeOut is None:
            raise TransactionFailure('unable to generate mundo fee')
        # EVR change out to Mundo
        if feeSats > 0:
            # use Mundo-provided fee calculation
            currencyChange = feeSatsReserved - feeSats
            if currencyChange <= 0:
                raise TransactionFailure('unable to generate currency change')
            currencyChangeOut = self._compileCurrencyOutputs(
                address=changeAddress, sats=currencyChange)[0]
        else:
            # fall back to local fee estimation
            currencyChangeOut, currencyChange = self._compileCurrencyChangeOutput(
                gatheredCurrencySats=feeSatsReserved,
                inputCount=len(gatheredSatoriUnspents),
                outputCount=len(satoriOuts) + 2 +
                (1 if satoriChangeOut is not None else 0),
                scriptPubKey=self._generateScriptPubKeyFromAddress(changeAddress),
                returnSats=True)
        if currencyChangeOut is None:
            raise TransactionFailure('unable to generate currency change')
        tx = self._createPartialOriginatorSimple(
            txins=txins,
            txinScripts=txinScripts,
            txouts=satoriOuts + [
                x for x in [satoriChangeOut]
                if x is not None] + [mundoFeeOut, currencyChangeOut])
        reportedFeeSats = feeSatsReserved - currencyChange
        return tx.serialize(), reportedFeeSats, self._txToHex(tx)

    def satoriOnlyCompleterSimple(
        self,
        serialTx: bytes,
        feeSatsReserved: int,
        reportedFeeSats: int,
        changeAddress: Union[str, None] = None,
        completerAddress: Union[str, None] = None,
        claimAddress: Union[str, None] = None,
        bridgeTransaction: bool = False,
        signOnly: bool = False,
    ) -> str:
        '''
        a companion function to satoriOnlyPartialSimple which completes the
        transaction by injecting the necessary rvn inputs to cover the fee.
        address is the address claim satori fee address.
        '''
        def _verifyFee():
            '''
            notice, currency change is guaranteed:
                reportedFeeSats < TxUtils.asSats(1)
                feeSatsReserved is greater than TxUtils.asSats(1)
            '''

            if bridgeTransaction:
                return (
                    reportedFeeSats < TxUtils.asSats(1) and
                    reportedFeeSats < feeSatsReserved and
                    tx.vout[-2].nValue == feeSatsReserved - reportedFeeSats)
            return (
                reportedFeeSats < TxUtils.asSats(1) and
                reportedFeeSats < feeSatsReserved and
                tx.vout[-1].nValue == feeSatsReserved - reportedFeeSats)

        def _verifyClaim():
            if bridgeTransaction:
                # [mundoFeeOut, bridgeFeeOut, currencyChangeOut, memoOut]
                bridgePaid = False
                mundoPaid = False
                for x in tx.vout:
                    if self._checkSatoriValue(x, self.bridgeFee):
                        bridgePaid = True
                    if self._checkSatoriValue(x, self.mundoFee):
                        mundoPaid = True
                return bridgePaid and mundoPaid
            # [mundoFeeOut, currencyChangeOut]
            mundoPaid = False
            for x in tx.vout:
                if self._checkSatoriValue(x, self.mundoFee):
                    mundoPaid = True
            return mundoPaid

        def _verifyClaimAddress():
            ''' verify the claim output goes to claimAddress (or completerAddress) '''
            target = claimAddress or completerAddress
            if bridgeTransaction:
                for i, x in enumerate(tx.vout[-4].scriptPubKey):
                    if i == 2 and isinstance(x, bytes):
                        return target == self.hash160ToAddress(x)
                return False
            for i, x in enumerate(tx.vout[-2].scriptPubKey):
                if i == 2 and isinstance(x, bytes):
                    return target == self.hash160ToAddress(x)
            return False

        def _verifyChangeAddress():
            ''' verify the change output goes to us at changeAddress '''
            if bridgeTransaction:
                for i, x in enumerate(tx.vout[-2].scriptPubKey):
                    if i == 2 and isinstance(x, bytes):
                        return changeAddress == self.hash160ToAddress(x)
                return False
            for i, x in enumerate(tx.vout[-1].scriptPubKey):
                if i == 2 and isinstance(x, bytes):
                    return changeAddress == self.hash160ToAddress(x)
            return False

        completerAddress = completerAddress or self.address
        logging.debug('completer', completerAddress)
        changeAddress = changeAddress or self.address
        logging.debug('change', changeAddress)
        tx = self._deserialize(serialTx)
        if not _verifyFee():
            raise TransactionFailure(
                f'fee mismatch, {reportedFeeSats}, {feeSatsReserved}')
        if not _verifyClaim():
            if bridgeTransaction:
                raise TransactionFailure(
                    f'bridge claim mismatch, {tx.vout[-4]}, {tx.vout[-3]}')
            raise TransactionFailure(f'claim mismatch, {tx.vout[-2]}')
        if not _verifyClaimAddress():
            raise TransactionFailure('claim mismatch, _verifyClaimAddress')
        if not _verifyChangeAddress():
            raise TransactionFailure('claim mismatch, _verifyChangeAddress')
        # add rvn fee input
        gatheredCurrencyUnspent = self._gatherReservedCurrencyUnspent(
            exactSats=feeSatsReserved)
        logging.debug('gathered', gatheredCurrencyUnspent)
        if gatheredCurrencyUnspent is None:
            raise TransactionFailure(f'unable to find sats {feeSatsReserved}')
        txins, txinScripts = self._compileInputs(
            gatheredCurrencyUnspents=[gatheredCurrencyUnspent])
        tx = self._createPartialCompleterSimple(
            tx=tx,
            txins=txins,
            txinScripts=txinScripts)
        txHex = self._txToHex(tx)
        if signOnly:
            return txHex
        return self.broadcast(txHex)

    def sendAllTransaction(
        self,
        address: str,
        feeOverride: Optional[int] = None,
    ) -> str:
        '''
        sweeps all Satori and currency to the address. so it has to take the fee
        out of whatever is in the wallet rather than tacking it on at the end.
        '''
        if not Validate.address(address, self.symbol):
            raise TransactionFailure('sendAllTransaction')
        # logging.debug('currency', self.currency,
        #              'self.reserve', self.reserve, color='yellow')
        if self.currency < self.reserve:
            raise TransactionFailure(
                'sendAllTransaction: not enough currency for fee')
        # grab everything
        gatheredSatoriUnspents = [
            x for x in self.unspentAssets if x.get('name', x.get('asset')) == 'SATORI']
        gatheredCurrencyUnspents = self.unspentCurrency
        currencySats = sum([x.get('value') for x in gatheredCurrencyUnspents])
        # compile inputs
        if len(gatheredSatoriUnspents) > 0:
            txins, txinScripts = self._compileInputs(
                gatheredCurrencyUnspents=gatheredCurrencyUnspents,
                gatheredSatoriUnspents=gatheredSatoriUnspents)
        else:
            txins, txinScripts = self._compileInputs(
                gatheredCurrencyUnspents=gatheredCurrencyUnspents)
        # determin how much currency to send: take out fee
        fee = feeOverride or TxUtils.estimatedFee(
            inputCount=(
                len(gatheredSatoriUnspents) +
                len(gatheredCurrencyUnspents)),
            outputCount=2)
        currencySatsLessFee = currencySats - fee
        if currencySatsLessFee < 0:
            raise TransactionFailure('tx: not enough currency to send')
        # since it's a send all, there's no change outputs
        if len(gatheredSatoriUnspents) > 0:
            txouts = (
                self._compileSatoriOutputs({
                    address: TxUtils.roundSatsDownToDivisibility(
                        sats=TxUtils.asSats(self.balance.amount),
                        divisibility=self.divisibility)}) +
                (
                    self._compileCurrencyOutputs(address=address, sats=currencySatsLessFee)
                    if currencySatsLessFee > 0 else []))
        else:
            txouts = self._compileCurrencyOutputs(address=address, sats=currencySatsLessFee)
        tx = self._createTransaction(
            txins=txins,
            txinScripts=txinScripts,
            txouts=txouts)
        requiredFee = TxUtils.getTxFee(self._txToHex(tx), TxUtils.feeRate)
        print('estimated fee:', fee, 'actual fee:', requiredFee)
        if requiredFee * 0.99 < fee < requiredFee * 1.25:
            return self.broadcast(self._txToHex(tx))
        return self.sendAllTransaction(
            address=address,
            feeOverride=requiredFee)

    # not finished
    # I thought this would be worth it, but
    # SIGHASH_ANYONECANPAY | SIGHASH_SIGNLE is still too complex. particularly
    # generating outputs
    # def sendAllPartial(self, address: str) -> str:
    #    '''
    #    sweeps all Satori and currency to the address. so it has to take the fee
    #    out of whatever is in the wallet rather than tacking it on at the end.
    #
    #    this one doesn't actaully need change back, so we could use the most
    #    general solution of SIGHASH_ANYONECANPAY | SIGHASH_SIGNLE if the server
    #    knows how to handle it.
    #    '''
    #    def _generateOutputs():
    #        '''
    #        we must guarantee we have the same number of inputs to outputs.
    #        we must guarantee sum of ouputs = sum of inputs - mundoFee.
    #        that is all.
    #
    #        we could run into a situation where we need to take the fee out of
    #        multiple inputs. We could also run into the situation where we need
    #        to pair a currency output with a satori input.
    #        '''
    #        reservedFee = 0
    #        outs = []
    #        mundoFeeSats = TxUtils.asSats(self.mundoFee)
    #        for x in gatheredCurrencyUnspents:
    #            if x.get('value') > reservedFee:
    #        for x in gatheredSatoriUnspents:
    #            if reservedFee < mundoFeeSats:
    #                if x.get('value') > mundoFeeSats - reservedFee:
    #                    reservedFee += (mundoFeeSats - reservedFee)
    #                    # compile output with
    #                    mundoFeeSats x.get('value') -
    #                reservedFee = x.get('value') -
    #        return ( # not finished, combine with above
    #            self._compileSatoriOutputs({
    #                address: unspent.get('x') - self.mundoFee # on first item
    #                for unspent in gatheredSatoriUnspents
    #                }) +
    #            self._compileCurrencyOutputs(currencySats, address))
    #
    #    if not Validate.address(address, self.symbol):
    #        raise TransactionFailure('sendAllTransaction')
    #    logging.debug('currency', self.currency,
    #                'self.reserve', self.reserve, color='yellow')
    #    if self.balance.amount <= self.mundoFee*2:
    #        # what if they have 2 satoris in 2 different utxos?
    #        # one goes to the destination, and what about the other?
    #        # server supplies the fee claim so... we can't create this
    #        # transaction unless we supply the fee claim, and the server detects
    #        # it.
    #        raise TransactionFailure(
    #            'sendAllTransaction: not enough Satori for fee')
    #    # grab everything
    #    gatheredSatoriUnspents = [
    #        x for x in self.unspentAssets if x.get('name', x.get('asset')) == 'SATORI']
    #    gatheredCurrencyUnspents = self.unspentCurrency
    #    currencySats = sum([x.get('value') for x in gatheredCurrencyUnspents])
    #    # compile inputs
    #    txins, txinScripts = self._compileInputs(
    #        gatheredCurrencyUnspents=gatheredCurrencyUnspents,
    #        gatheredSatoriUnspents=gatheredSatoriUnspents)
    #    # since it's a send all, there's no change outputs
    #    tx = self._createPartialOriginator(
    #        txins=txins,
    #        txinScripts=txinScripts,
    #        txouts=_generateOutputs())
    #    return tx.serialize()

    def sendAllPartialSimple(
        self,
        address: str,
        feeSatsReserved: int = 0,
        feeSats: int = 0,
        satoriFeeAmount: int = 0,
        completerAddress: str = None,
        changeAddress: str = None,
        satoriFeeAddress: str = None,
        **kwargs
    ) -> tuple[str, int]:
        '''
        sweeps all Satori and currency to the address. so it has to take the fee
        out of whatever is in the wallet rather than tacking it on at the end.

        this one doesn't actaully need change back, so we could use the most
        general solution of SIGHASH_ANYONECANPAY | SIGHASH_SIGNLE if the server
        knows how to handle it.
        '''
        if completerAddress is None or changeAddress is None or feeSatsReserved == 0:
            raise TransactionFailure('need completer details')
        if not Validate.address(address, self.symbol):
            raise TransactionFailure('sendAllTransaction')
        # use Mundo-provided SATORI fee, fall back to hardcoded default
        mundoFeeSats = satoriFeeAmount if satoriFeeAmount > 0 else TxUtils.asSats(self.mundoFee)
        mundoFeeFloat = TxUtils.asAmount(mundoFeeSats)
        if self.balance.amount < mundoFeeFloat:
            raise TransactionFailure(
                'sendAllTransaction: not enough Satori for fee')
        # grab everything
        gatheredSatoriUnspents = [
            x for x in self.unspentAssets if x.get('name', x.get('asset')) == 'SATORI']
        gatheredCurrencyUnspents = self.unspentCurrency
        currencySats = sum([x.get('value') for x in gatheredCurrencyUnspents])
        # compile inputs
        txins, txinScripts = self._compileInputs(
            gatheredCurrencyUnspents=gatheredCurrencyUnspents,
            gatheredSatoriUnspents=gatheredSatoriUnspents)
        feeAddr = satoriFeeAddress or completerAddress
        sweepOuts = (
            (
                self._compileCurrencyOutputs(address=address, sats=currencySats)
                if currencySats > 0 else []) +
            self._compileSatoriOutputs(
                {address:
                    TxUtils.roundSatsDownToDivisibility(
                        sats=TxUtils.asSats(
                            self.balance.amount) - mundoFeeSats,
                        divisibility=self.divisibility)}))
        mundoFeeOut = self._compileSatoriOutputs(
            {feeAddr: mundoFeeSats})[0]
        # EVR change out to Mundo
        if feeSats > 0:
            currencyChange = feeSatsReserved - feeSats
            if currencyChange <= 0:
                raise TransactionFailure('unable to generate currency change')
            currencyChangeOut = self._compileCurrencyOutputs(
                address=changeAddress, sats=currencyChange)[0]
        else:
            currencyChangeOut, currencyChange = self._compileCurrencyChangeOutput(
                gatheredCurrencySats=feeSatsReserved,
                inputCount=len(gatheredSatoriUnspents) +
                len(gatheredCurrencyUnspents),
                outputCount=len(sweepOuts) + 2,
                scriptPubKey=self._generateScriptPubKeyFromAddress(changeAddress),
                returnSats=True)
        # since it's a send all, there's no change outputs
        tx = self._createPartialOriginatorSimple(
            txins=txins,
            txinScripts=txinScripts,
            txouts=sweepOuts + [mundoFeeOut, currencyChangeOut])
        reportedFeeSats = feeSatsReserved - currencyChange
        return tx.serialize(), reportedFeeSats, self._txToHex(tx)

    def typicalNeuronTransaction(
        self,
        amount: float,
        address: str,
        sweep: bool = False,
        requestSimplePartialFn: callable = None,
        broadcastSimplePartialFn: callable = None,
    ) -> TransactionResult:

        def sendDirect():
            if sweep:
                txid = self.sendAllTransaction(address)
            else:
                txid = self.satoriTransaction(amount=amount, address=address)
            if isinstance(txid, str) and len(txid) == 64:
                return TransactionResult(
                    result=None,
                    success=True,
                    msg=txid)
            return TransactionResult(
                result=None,
                success=False,
                msg=f'Send Failed: {txid}')

        def sendIndirect():
            evrRequiredMsg = 'No EVR in wallet, EVR is required as a transaction fee to send SATORI.'
            if requestSimplePartialFn is None or broadcastSimplePartialFn is None:
                return TransactionResult(
                    result=None,
                    success=False,
                    msg=evrRequiredMsg)
            try:
                # estimate input/output counts for Mundo fee calculation
                satoriSats = TxUtils.asSats(amount) if not sweep else TxUtils.asSats(self.balance.amount)
                try:
                    gatheredSatoriUnspents, _ = self._gatherSatoriUnspents(satoriSats)
                except Exception:
                    gatheredSatoriUnspents = self.unspentAssets
                estimatedInputCount = len(gatheredSatoriUnspents)
                estimatedOutputCount = 3  # recipient + satori change + mundo fee

                responseJson = requestSimplePartialFn(
                    network='evrmore',
                    inputCount=estimatedInputCount,
                    outputCount=estimatedOutputCount)
                changeAddress = responseJson.get('changeAddress')
                feeSatsReserved = responseJson.get('feeSatsReserved')
                feeSats = responseJson.get('feeSats', 0)
                completerAddress = responseJson.get('completerAddress')
                satoriFeeAddress = responseJson.get('satoriFeeAddress')
                satoriFeeAmount = responseJson.get('satoriFeeAmount', 0)
                if feeSatsReserved == 0 or completerAddress is None:
                    return TransactionResult(
                        result=None,
                        success=False,
                        msg=evrRequiredMsg)
                mundoFeeFloat = TxUtils.asAmount(satoriFeeAmount) if satoriFeeAmount > 0 else self.mundoFee
                sendPartialFunction = self.satoriOnlyPartialSimple
                if sweep:
                    sendPartialFunction = self.sendAllPartialSimple
                tx, reportedFeeSats, txhex = sendPartialFunction(
                    amount=float(amount),
                    address=address,
                    changeAddress=changeAddress,
                    feeSatsReserved=feeSatsReserved,
                    feeSats=feeSats,
                    completerAddress=completerAddress,
                    satoriFeeAddress=satoriFeeAddress,
                    satoriFeeAmount=satoriFeeAmount,
                    pullFeeFromAmount=float(amount) + mundoFeeFloat > self.balance.amount)
                if (
                    tx is not None and
                    reportedFeeSats is not None and
                    reportedFeeSats > 0
                ):
                    # Mundo signs with signOnly=true, returns signed tx hex
                    signedTxHex = broadcastSimplePartialFn(
                        tx=tx,
                        reportedFeeSats=reportedFeeSats,
                        feeSatsReserved=feeSatsReserved,
                        network='evrmore')
                    if signedTxHex and len(signedTxHex) > 0:
                        # broadcast the fully signed tx ourselves
                        txid = self.broadcast(signedTxHex)
                        if isinstance(txid, str) and len(txid) == 64:
                            return TransactionResult(
                                result=None,
                                success=True,
                                msg=txid)
                        return TransactionResult(
                            result=None,
                            success=False,
                            msg=f'Send Failed: broadcast error: {txid}')
                    return TransactionResult(
                        result=None,
                        success=False,
                        msg='Send Failed: Mundo signing failed.')
                return TransactionResult(
                    result=None,
                    success=False,
                    msg='unable to generate transaction')
            except Exception:
                # Mundo unreachable — fall back to EVR-required message
                return TransactionResult(
                    result=None,
                    success=False,
                    msg=evrRequiredMsg)

        ready, partialReady, msg = self.validateForTypicalNeuronTransaction(
            amount=amount,
            address=address,
            sweep=sweep)
        if ready:
            return sendDirect()
        elif partialReady:
            return sendIndirect()
        return TransactionResult(
            result=None,
            success=False,
            msg=f'Send Failed: {msg}')

    def typicalNeuronBridgeTransaction(
        self,
        amount: float,
        ethAddress: str,
        chain: str = 'base',
        requestSimplePartialFn: callable = None,
        ofacReportedFn: callable = None,
        broadcastBridgeSimplePartialFn: callable = None,
    ) -> TransactionResult:

        def sendDirect():
            txhex = self.satoriDistribution(
                amountByAddress={
                    self.bridgeAddress: self.bridgeFee,
                    self.burnAddress: amount},
                memo=f'{chain}:{ethAddress}',
                broadcast=False)
            if not ofacReportedFn(txid=TxUtils.txhexToTxid(txhex)):
                return TransactionResult(
                    result=None,
                    success=False,
                    msg='Send Failed: OFAC on Report')
            txid = self.broadcast(txhex)
            if len(txid) == 64:
                return TransactionResult(
                    result=None,
                    success=True,
                    msg=txid)
            return TransactionResult(
                result=None,
                success=False,
                msg=f'Send Failed: {txid}')

        def sendIndirect():
            responseJson = requestSimplePartialFn(network='main')
            changeAddress = responseJson.get('changeAddress')
            feeSatsReserved = responseJson.get('feeSatsReserved')
            completerAddress = responseJson.get('completerAddress')
            if feeSatsReserved == 0 or completerAddress is None:
                return TransactionResult(
                    result='try again',
                    success=True,
                    tx=None,
                    msg='creating partial, need feeSatsReserved.')
            tx, reportedFeeSats, txhex = self.satoriOnlyBridgePartialSimple(
                amount=amount,
                ethAddress=ethAddress,
                changeAddress=changeAddress,
                feeSatsReserved=feeSatsReserved,
                completerAddress=completerAddress)
            if ( # checking any on of these should suffice in theory...
                tx is not None and
                reportedFeeSats is not None and
                reportedFeeSats > 0
            ):
                txid = TxUtils.txhexToTxid(txhex)
                if not ofacReportedFn(txid):
                    return TransactionResult(
                        result=None,
                        success=False,
                        msg='Send Failed: OFAC on Report')
                r = broadcastBridgeSimplePartialFn(
                    tx=tx,
                    reportedFeeSats=reportedFeeSats,
                    feeSatsReserved=feeSatsReserved,
                    walletId=responseJson.get('partialId'),
                    network='evrmore')
                if r.text.startswith('{"code":1,"message":'):
                    return TransactionResult(
                        result=None,
                        success=False,
                        msg=f'Send Failed: {r.json().get("message")}')
                elif r.text != '':
                    return TransactionResult(
                        result=txid,
                        success=True,
                        msg=r.text)
                else:
                    return TransactionResult(
                        result=None,
                        success=False,
                        msg='Send Failed: and try again in a few minutes.')
            return TransactionResult(
                result=None,
                success=False,
                msg='unable to generate transaction')

        ready, partialReady, msg = self.validateForTypicalNeuronBridgeTransaction(
            amount=amount,
            ethAddress=ethAddress,
            chain=chain)
        if ready:
            return sendDirect()
        elif partialReady:
            return sendIndirect()
        return TransactionResult(
            result=None,
            success=False,
            msg=f'Send Failed: {msg}')



    def validateForTypicalNeuronTransaction(
        self,
        amount: float,
        address: str,
        sweep: bool = False,
    ) -> TransactionResult:
        if not sweep:
            if isinstance(amount, Decimal):
                amount = float(amount)
            if amount <= 0:
                return False, False, f'Satori Transaction bad params: unable to send amount: {amount}'
            if amount > self.balance.amount:
                return False, False, f'Satori Transaction bad params: {amount} > {self.balance.amount} '
        if not Validate.address(address, self.symbol):
            return False, False, f'Satori Transaction bad params: address: {address}'
        if self.currency < self.reserve:
            if not sweep and amount > self.balance.amount + self.mundoFee:
                return False, False, f'Satori Transaction bad params: not enough for transaction fees: {amount} > {self.balance.amount} + {self.mundoFee}'
            return False, True, 'currency < reserve'
        return True, False, ''

    def validateForTypicalNeuronBridgeTransaction(
        self,
        amount: float,
        ethAddress: str,
        chain: str = 'base',
    ) -> tuple[bool, bool, str]: # success, partial-ready, msg
        if isinstance(amount, Decimal):
            amount = float(amount)
        if chain != 'base':
            return False, False, 'can only bridge to base'
        if amount <= 0:
            return False, False, f'Satori Bridge Transaction bad params: unable to send amount: {amount}'
        if amount > self.maxBridgeAmount:
            return False, False, 'Satori Bridge Transaction bad params: amount > 100'
        if not Validate.ethAddress(ethAddress):
            return False, False, f'Satori Bridge Transaction bad params: eth address: {ethAddress}'
        if self.balance.amount < amount + self.bridgeFee:
            return False, False, f'Satori Bridge Transaction bad params: balance too low to pay for bridgeFee {self.balance.amount} < {amount} + {self.bridgeFee}'
        if self.currency < self.reserve:
            return False, True, 'currency < reserve'
        return True, False, ''
