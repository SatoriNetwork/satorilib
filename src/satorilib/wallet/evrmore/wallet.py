from functools import partial
from typing import Any, Union, Callable, Dict, Optional
import time
import datetime as dt
from evrmore import SelectParams
from evrmore.wallet import P2PKHEvrmoreAddress, CEvrmoreAddress, CEvrmoreSecret, P2SHEvrmoreAddress
from evrmore.core.scripteval import VerifyScript, SCRIPT_VERIFY_P2SH
from evrmore.core.script import (
    CScript, OP_DUP, OP_HASH160, OP_EQUALVERIFY, OP_CHECKSIG, SignatureHash, SIGHASH_ALL, 
    OP_EVR_ASSET, OP_DROP, OP_RETURN, SIGHASH_ANYONECANPAY, OP_IF, OP_ELSE, OP_ENDIF, 
    OP_CHECKMULTISIG, OP_CHECKLOCKTIMEVERIFY, OP_CHECKSEQUENCEVERIFY)
from evrmore.core import b2lx, b2x, lx, COutPoint, CMutableTxOut, CMutableTxIn, CMutableTransaction, Hash160
from evrmore.core.scripteval import EvalScriptError
from satorilib import logging
from satorilib.electrumx import Electrumx
from satorilib.wallet.concepts.transaction import AssetTransaction, TransactionFailure
from satorilib.wallet.utils.transaction import TxUtils
from satorilib.wallet.utils.validate import Validate
from satorilib.wallet.wallet import Wallet
from satorilib.wallet.evrmore.sign import signMessage
from satorilib.wallet.evrmore.verify import verify
from satorilib.wallet.evrmore.valid import isValidEvrmoreAddress
from satorilib.wallet.identity import Identity
from satorilib.wallet.evrmore.identity import EvrmoreIdentity
from satorilib.wallet.rpc_methods import RpcMethodsMixin

class EvrmoreWallet(Wallet, RpcMethodsMixin):

    @staticmethod
    def addressIsValid(address: str) -> bool:
        return isValidEvrmoreAddress(address)

    @staticmethod
    def create(
        walletPath: Union[str,None] = None,
        cachePath: Union[str,None] = None,
        password: Union[str,None] = None,
        identity: Union[Identity, None] = None,
        reserve: float = 0,
        watchAssets: list[str] = None,
        skipSave: bool = False,
        pullFullTransactions: bool = True,
        balanceUpdatedCallback: Union[Callable, None] = None,
        electrumx: Electrumx = None,
        hostPort: str = None,
        persistent: bool = False,
        cachedPeersFile: Union[str, None] = None,
        rpcNodes: Union[list, None] = None,
        rpcUrl: Union[str, None] = None,
        rpcUser: Union[str, None] = None,
        rpcPassword: Union[str, None] = None,
    ) -> 'EvrmoreWallet':
        return EvrmoreWallet(
            identity=identity or EvrmoreIdentity(walletPath=walletPath, password=password),
            electrumx=electrumx or Electrumx.create(
                hostPort=hostPort,
                persistent=persistent,
                cachedPeersFile=cachedPeersFile),
            cachePath=cachePath,
            reserve=reserve,
            watchAssets=watchAssets,
            skipSave=skipSave,
            pullFullTransactions=pullFullTransactions,
            hostPort=hostPort,
            persistent=persistent,
            balanceUpdatedCallback=balanceUpdatedCallback,
            cachedPeersFile=cachedPeersFile,
            rpcNodes=rpcNodes,
            rpcUrl=rpcUrl,
            rpcUser=rpcUser,
            rpcPassword=rpcPassword)

    def __init__(
        self,
        identity: EvrmoreIdentity,
        electrumx: Union[Electrumx, None] = None,
        cachePath: Union[str, None] = None,
        reserve: float = 0,
        watchAssets: list[str] = None,
        skipSave: bool = False,
        pullFullTransactions: bool = True,
        balanceUpdatedCallback: Union[Callable, None] = None,
        **kwargs
    ):
        super().__init__(
            identity=identity,
            cachePath=cachePath,
            electrumx=electrumx,
            reserve=reserve,
            watchAssets=watchAssets,
            skipSave=skipSave,
            pullFullTransactions=pullFullTransactions,
            balanceUpdatedCallback=balanceUpdatedCallback)

        # Initialize RPC client if config provided
        self._initRpcClient(
            rpcNodes=kwargs.get('rpcNodes'),
            rpcUrl=kwargs.get('rpcUrl'),
            rpcUser=kwargs.get('rpcUser'),
            rpcPassword=kwargs.get('rpcPassword'),
        )

    def maybeConnect(self, electrumx = None):
        if self.electrumx is None:
            self.electrumx = Electrumx.create(
                    hostPort=self.hostPort, 
                    persistent= self.persistent,
                    cachedPeersFile=self.cachedPeersFile)
            return self.electrumx is not None
        elif self.electrumx.isConnected:
            return True
        else:
            if self.electrumx.reconnect():
                return True
            else:
                self.electrumx = None
                return self.maybeConnect(electrumx)

    @property
    def symbol(self) -> str:
        return 'evr'

    @property
    def chain(self) -> str:
        return 'Evrmore'

    @property
    def networkByte(self) -> bytes:
        return self.networkByteP2PKH

    @property
    def networkByteP2PKH(self) -> bytes:
        # evrmore.params.BASE58_PREFIXES['PUBKEY_ADDR']
        # BASE58_PREFIXES = {'PUBKEY_ADDR': 33,
        #                   'SCRIPT_ADDR': 92,
        #                   'SECRET_KEY': 128}
        # RVN = return b'\x3c'  # b'0x3c'
        return (33).to_bytes(1, 'big')

    @property
    def networkByteP2SH(self) -> bytes:
        return (92).to_bytes(1, 'big')

    @property
    def satoriOriginalTxHash(self) -> str:
        # SATORI/TEST 15dd33886452c02d58b500903441b81128ef0d21dd22502aa684c002b37880fe
        return 'df745a3ee1050a9557c3b449df87bdd8942980dff365f7f5a93bc10cb1080188'

    @property
    def ethaddress(self) -> Union[str, None]:
        try:
            account = self.account
            return (
                account.checksum_address
                if hasattr(account, 'checksum_address') else None
            ) or account.address
        except Exception as e:
            logging.error(e)
            return None

    # signature ###############################################################

    def sign(self, message: str):
        return signMessage(self.identity._privateKeyObj, message)

    def verify(self, message: str, sig: bytes, address: Union[str, None] = None):
        return verify(
            message=message,
            signature=sig,
            address=address or self.address)

    def generateOtp(self, msg: str = '') -> str:
        ''' generate a one-time password using the wallet '''
        return self.identity.generateCompressedOtpPayload(msg)
    
    # generation ##############################################################

    @staticmethod
    def generateAddress(pubkey: Union[bytes, str]) -> str:
        if isinstance(pubkey, str):
            pubkey = bytes.fromhex(pubkey)
        return str(P2PKHEvrmoreAddress.from_pubkey(pubkey))

    @staticmethod
    def generateP2SHAddress(redeem_script: CScript) -> str:
        """Generate a P2SH address from a redeem script."""
        return str(P2SHEvrmoreAddress.from_redeemScript(redeem_script))

    def _generatePrivateKey(self, compressed: bool = True, privkey: Union[str, bytes, None] = None):
        SelectParams('mainnet')
        if not self._entropy:
            privkey = privkey or self.privateKey
        if privkey:
            if isinstance(privkey, str):
                #return CEvrmoreSecret.from_secret_bytes(bytes.fromhex(privkey), compressed=compressed) # bytes below
                #return CEvrmoreSecret.from_hex(privkey) # probably not hex
                return CEvrmoreSecret(privkey)
            elif isinstance(privkey, bytes):
                return CEvrmoreSecret.from_secret_bytes(privkey, compressed=compressed)
            else:
                raise ValueError('privkey must be a string or bytes')
        return CEvrmoreSecret.from_secret_bytes(self._entropy, compressed=compressed)

    def _generateAddress(self, pub=None):
        return P2PKHEvrmoreAddress.from_pubkey(pub or self.identity._privateKeyObj.pub)

    def _generateScriptPubKeyFromAddress(self, address: str):
        return CEvrmoreAddress(address).to_scriptPubKey()

    # transaction creation ####################################################

    def _checkSatoriValue(self, output: CMutableTxOut, amount: float=None) -> bool:
        '''
        returns true if the output is a satori output of amount or self.mundoFee
        '''
        nextOne = False
        for i, x in enumerate(output.scriptPubKey):
            if nextOne:
                # doesn't padd with 0s at the end
                # b'rvnt\x06SATORI\x00\xe1\xf5\x05'
                # b'rvnt\x06SATORI\x00\xe1\xf5\x05\x00\x00\x00\x00'
                expected = bytes.fromhex(
                    AssetTransaction.satoriHex(self.symbol) +
                    TxUtils.padHexStringTo8Bytes(
                        TxUtils.intToLittleEndianHex(
                            TxUtils.asSats(amount or self.mundoFee))))
                # handle both formats: with and without asset protocol
                # length prefix byte (e.g. 0x13 for 19-byte SATORI data)
                if x.startswith(expected):
                    return True
                if len(x) > 1 and x[1:].startswith(expected):
                    return True
                return False
            if x == OP_EVR_ASSET:
                nextOne = True
        return False

    def _compileInputs(
        self,
        gatheredCurrencyUnspents: list = None,
        gatheredSatoriUnspents: list = None,
        redeem_scripts: dict[str, CScript] = None,  # Map of tx_hash:pos to redeem script
    ) -> tuple[list, list]:
        # currency vins
        txins = []
        txinScripts = []
        for utxo in (gatheredCurrencyUnspents or []):
            tx_hash = utxo.get('tx_hash')
            tx_pos = utxo.get('tx_pos')
            txin = CMutableTxIn(COutPoint(lx(tx_hash), tx_pos))
            
            # If we have a scriptPubKey in the UTXO, use it directly
            if 'scriptPubKey' in utxo:
                txinScriptPubKey = CScript(bytes.fromhex(utxo.get('scriptPubKey')))
            else:
                # No scriptPubKey provided, we need to construct one
                utxo_key = f"{tx_hash}:{tx_pos}"
                if redeem_scripts and utxo_key in redeem_scripts:
                    # Construct P2SH scriptPubKey from redeem script
                    redeem_script = redeem_scripts[utxo_key]
                    txinScriptPubKey = P2SHEvrmoreAddress.from_redeemScript(redeem_script).to_scriptPubKey()
                else:
                    # Construct standard P2PKH scriptPubKey
                    txinScriptPubKey = CScript([
                        OP_DUP,
                        OP_HASH160,
                        Hash160(self.publicKeyBytes),
                        OP_EQUALVERIFY,
                        OP_CHECKSIG])
            txins.append(txin)
            txinScripts.append(txinScriptPubKey)

        # satori vins
        for utxo in (gatheredSatoriUnspents or []):
            tx_hash = utxo.get('tx_hash')
            tx_pos = utxo.get('tx_pos')
            txin = CMutableTxIn(COutPoint(lx(tx_hash), tx_pos))
            
            # If we have a scriptPubKey in the UTXO, use it directly
            if 'scriptPubKey' in utxo:
                txinScriptPubKey = CScript(bytes.fromhex(utxo.get('scriptPubKey')))
            else:
                # No scriptPubKey provided, we need to construct one
                utxo_key = f"{tx_hash}:{tx_pos}"
                if redeem_scripts and utxo_key in redeem_scripts:
                    # Construct P2SH scriptPubKey from redeem script and add asset data
                    redeem_script = redeem_scripts[utxo_key]
                    base_script = P2SHEvrmoreAddress.from_redeemScript(redeem_script).to_scriptPubKey()
                    txinScriptPubKey = CScript([
                        *base_script,
                        OP_EVR_ASSET,
                        bytes.fromhex(
                            AssetTransaction.satoriHex(self.symbol) +
                            TxUtils.padHexStringTo8Bytes(
                                TxUtils.intToLittleEndianHex(int(utxo.get('value'))))),
                        OP_DROP])
                else:
                    # Construct standard P2PKH scriptPubKey with asset data
                    txinScriptPubKey = CScript([
                        OP_DUP,
                        OP_HASH160,
                        Hash160(self.publicKeyBytes),
                        OP_EQUALVERIFY,
                        OP_CHECKSIG,
                        OP_EVR_ASSET,
                        bytes.fromhex(
                            AssetTransaction.satoriHex(self.symbol) +
                            TxUtils.padHexStringTo8Bytes(
                                TxUtils.intToLittleEndianHex(int(utxo.get('value'))))),
                        OP_DROP])
            txins.append(txin)
            txinScripts.append(txinScriptPubKey)
        return txins, txinScripts

    def _compileSatoriOutputs(self, satsByAddress: dict[str, int] = None) -> list:
        txouts = []
        for address, sats in satsByAddress.items():
            txout = CMutableTxOut(
                0,
                CScript([
                    *CEvrmoreAddress(address).to_scriptPubKey(),
                    OP_EVR_ASSET,
                    bytes.fromhex(
                        AssetTransaction.satoriHex(self.symbol) +
                        TxUtils.padHexStringTo8Bytes(
                            TxUtils.intToLittleEndianHex(sats))),
                    OP_DROP]))
            txouts.append(txout)
        return txouts

    def _compileCurrencyOutput(self, address: str, sats: int) -> CMutableTxOut:
        return CMutableTxOut(
            sats,
            CEvrmoreAddress(address).to_scriptPubKey())

    def _compileCurrencyOutputs(self, satsByAddress: dict[str, int] = None, address: str = None, sats: int = None) -> list[CMutableTxOut]:
        return [self._compileCurrencyOutput(address, sats)] if address and sats else [self._compileCurrencyOutput(addr, s) for addr, s in satsByAddress.items()] if satsByAddress else []

    def _compileSatoriChangeOutput(
        self,
        satoriSats: int = 0,
        gatheredSatoriSats: int = 0,
        changeAddress: Optional[str] = None,
    ) -> Union[CMutableTxOut, None]:
        satoriChange = gatheredSatoriSats - satoriSats
        if satoriChange > 0:
            return CMutableTxOut(
                0,
                CScript([
                    *CEvrmoreAddress(changeAddress or self.address).to_scriptPubKey(),
                    OP_EVR_ASSET,
                    bytes.fromhex(
                        AssetTransaction.satoriHex(self.symbol) +
                        TxUtils.padHexStringTo8Bytes(
                            TxUtils.intToLittleEndianHex(satoriChange))),
                    OP_DROP]))
        if satoriChange < 0:
            raise TransactionFailure('tx: not enough satori to send')
        return None

    def _compileCurrencyChangeOutput(
        self,
        currencySats: int = 0,
        gatheredCurrencySats: int = 0,
        inputCount: int = 0,
        outputCount: int = 0,
        fee: int = 0,
        scriptPubKey: CScript = None,
        returnSats: bool = False,
        changeAddress: Optional[str] = None,
    ) -> Union[CMutableTxOut, None, tuple[CMutableTxOut, int]]:
        currencyChange = gatheredCurrencySats - currencySats - (
            fee or TxUtils.estimatedFee(
                inputCount=inputCount,
                outputCount=outputCount))
        if currencyChange > 0:
            if str(CEvrmoreAddress(self.address)) != self.address:
                raise TransactionFailure('tx: address mismatch')
            # allow for overrirde, should probably allow for override as address str:
            #if str(CEvrmoreAddress(self.address)).to_scriptPubKey() != scriptPubKey:
            #    raise TransactionFailure('tx: scriptPubKey mismatch')
            if CEvrmoreAddress(self.address).to_scriptPubKey() != self.identity._addressObj.to_scriptPubKey():
                raise TransactionFailure('tx: scriptPubKey mismatch')
            txout = CMutableTxOut(
                currencyChange,
                scriptPubKey or CEvrmoreAddress(changeAddress or self.address).to_scriptPubKey()) # self._addressObj.to_scriptPubKey())
            # use *CEvrmoreAddress(self.address).to_scriptPubKey()? # supports P2SH automatically
            if returnSats:
                return txout, currencyChange
            return txout
        if currencyChange < 0:
            # go back and get more?
            raise TransactionFailure('tx: not enough currency to send')
        return None

    def _compileMemoOutput(self, memo: str) -> Union[CMutableTxOut, None]:
        if memo is not None and memo != '' and 4 < len(memo) < 80:
            return CMutableTxOut(
                0,
                CScript([
                    OP_RETURN,
                    # it seems as though we can't do 4 or less
                    # probably because of something CScript is doing... idk why.
                    memo.encode()
                ]))
        return None

    def _createTransaction(
        self,
        txins: list,
        txinScripts: list,
        txouts: list,
        redeem_scripts: dict[str, CScript] = None,
        signatures: dict[str, list[bytes]] = None,  # Map of tx_hash:pos to list of signatures
    ) -> CMutableTransaction:
        tx = CMutableTransaction(txins, txouts)
        for i, (txin, txinScriptPubKey) in enumerate(zip(txins, txinScripts)):
            utxo_key = f"{b2x(txin.prevout.hash)}:{txin.prevout.n}"
            redeem_script = redeem_scripts.get(utxo_key) if redeem_scripts else None
            other_sigs = signatures.get(utxo_key) if signatures else None
            self._signInput(
                tx=tx,
                i=i,
                txin=txin,
                txinScriptPubKey=txinScriptPubKey,
                sighashFlag=SIGHASH_ALL,
                redeem_script=redeem_script,
                signatures=other_sigs)
        return tx

    def _createPartialOriginatorSimple(self, txins: list, txinScripts: list, txouts: list) -> CMutableTransaction:
        ''' simple version SIGHASH_ANYONECANPAY | SIGHASH_ALL '''
        tx = CMutableTransaction(txins, txouts)
        # logging.debug('txins', txins)
        # logging.debug('txouts', txouts)
        for i, (txin, txinScriptPubKey) in enumerate(zip(txins, txinScripts)):
            self._signInput(
                tx=tx,
                i=i,
                txin=txin,
                txinScriptPubKey=txinScriptPubKey,
                sighashFlag=SIGHASH_ANYONECANPAY | SIGHASH_ALL)
        return tx

    def _createPartialCompleterSimple(self, txins: list, txinScripts: list, tx: CMutableTransaction) -> CMutableTransaction:
        '''
        simple version SIGHASH_ANYONECANPAY | SIGHASH_ALL
        just adds an input for the RVN fee and signs it
        '''
        # todo, verify the last two outputs at somepoint before this
        tx.vin.extend(txins)
        startIndex = len(tx.vin) - len(txins)
        for i, (txin, txinScriptPubKey) in (
            enumerate(zip(tx.vin[startIndex:], txinScripts), start=startIndex)
        ):
            self._signInput(
                tx=tx,
                i=i,
                txin=txin,
                txinScriptPubKey=txinScriptPubKey,
                sighashFlag=SIGHASH_ANYONECANPAY | SIGHASH_ALL)
        return tx

    def _signInput(
        self,
        tx: CMutableTransaction,
        i: int,
        txin: CMutableTxIn,
        txinScriptPubKey: CScript,
        sighashFlag: int,
        redeem_script: CScript = None,
        signatures: list[bytes] = None,  # For multi-sig, list of signatures from other signers
    ):
        """Sign a transaction input.
        
        Args:
            tx: The transaction to sign
            i: Input index
            txin: The transaction input
            txinScriptPubKey: The scriptPubKey of the input
            sighashFlag: The sighash flag to use
            redeem_script: For P2SH inputs, the redeem script
            signatures: For multi-sig, list of signatures from other signers
        """
        if redeem_script:
            # This is a P2SH input
            sighash = SignatureHash(redeem_script, tx, i, sighashFlag)
            sig = self.identity._privateKeyObj.sign(sighash) + bytes([sighashFlag])
            
            if signatures:
                # Multi-sig case
                # Combine our signature with other signatures
                all_sigs = signatures + [sig]
                # Sort signatures by public key (required by Bitcoin)
                all_sigs.sort()
                # Create scriptSig: [sig1, sig2, ..., redeem_script]
                txin.scriptSig = CScript(all_sigs + [redeem_script])
            else:
                # Single-sig P2SH case
                txin.scriptSig = CScript([sig, redeem_script])
        else:
            # Regular P2PKH input
            sighash = SignatureHash(txinScriptPubKey, tx, i, sighashFlag)
            sig = self.identity._privateKeyObj.sign(sighash) + bytes([sighashFlag])
            txin.scriptSig = CScript([sig, self.identity._privateKeyObj.pub])

        try:
            # For P2SH, we need to verify against the redeem script
            script_to_verify = redeem_script if redeem_script else txinScriptPubKey
            VerifyScript(
                txin.scriptSig,
                script_to_verify,
                tx, i, (SCRIPT_VERIFY_P2SH,))
        except EvalScriptError as e:
            # python-ravencoinlib doesn't support OP_RVN_ASSET in txinScriptPubKey
            if str(e) != 'EvalScript: unsupported opcode 0xc0':
                raise EvalScriptError(e)

    # def _createPartialOriginator(self, txins: list, txinScripts: list, txouts: list) -> CMutableTransaction:
    #    ''' not completed - complex version SIGHASH_ANYONECANPAY | SIGHASH_SINGLE '''
    #    tx = CMutableTransaction(txins, txouts)
    #    for i, (txin, txinScriptPubKey) in enumerate(zip(tx.vin, txinScripts)):
    #        # Use SIGHASH_SINGLE for the originator's inputs
    #        sighash_type = SIGHASH_SINGLE
    #        sighash = SignatureHash(txinScriptPubKey, tx, i, sighash_type)
    #        sig = self._privateKeyObj.sign(sighash) + bytes([sighash_type])
    #        txin.scriptSig = CScript([sig, self._privateKeyObj.pub])
    #    return tx
    #
    # def _createPartialCompleter(self, txins: list, txinScripts: list, txouts: list, tx: CMutableTransaction) -> CMutableTransaction:
    #    ''' not completed '''
    #    tx.vin.extend(txins)  # Add new inputs
    #    tx.vout.extend(txouts)  # Add new outputs
    #    # Sign new inputs with SIGHASH_ANYONECANPAY and possibly SIGHASH_SINGLE
    #    # Assuming the completer's inputs start from len(tx.vin) - len(txins)
    #    startIndex = len(tx.vin) - len(txins)
    #    for i, (txin, txinScriptPubKey) in enumerate(zip(tx.vin[startIndex:], txinScripts), start=startIndex):
    #        sighash_type = SIGHASH_ANYONECANPAY  # Or SIGHASH_ANYONECANPAY | SIGHASH_SINGLE
    #        sighash = SignatureHash(txinScriptPubKey, tx, i, sighash_type)
    #        sig = self._privateKeyObj.sign(sighash) + bytes([sighash_type])
    #        txin.scriptSig = CScript([sig, self._privateKeyObj.pub])
    #    return tx

    def _txToHex(self, tx: CMutableTransaction) -> str:
        return b2x(tx.serialize())

    def _serialize(self, tx: CMutableTransaction) -> bytes:
        return tx.serialize()

    def _deserialize(self, serialTx: bytes) -> CMutableTransaction:
        return CMutableTransaction.deserialize(serialTx)

    ### p2sh infrastructure ################################################################

    @staticmethod
    def _cltvNumberFrom(dtOrInt: Union[dt.datetime, int]) -> int:
        if isinstance(dtOrInt, int):
            # block height or unix ts (caller ensures correct type)
            return dtOrInt
        if isinstance(dtOrInt, dt.datetime):
            d = dtOrInt if dtOrInt.tzinfo else dtOrInt.replace(tzinfo=dt.timezone.utc)
            ts = int(d.astimezone(dt.timezone.utc).timestamp())
            # CLTV timestamp must be >= 500,000,000
            if ts < 500_000_000:
                raise ValueError("CLTV timestamp must be >= 500,000,000")
            return ts
        raise TypeError("redeemDates value must be datetime or int")

    def _compileClaimOnP2SH(
        self,
        address: str,
        redeemScript: bytes,
        redeemParams: Callable,
        currencySats: float=0,
        satoriSats: float=0,
        feeOverride: Optional[int] = None,
        fundingTxId: str = None,
        fundingVout: int = None,
        fundingTxIds: list[str] = None,
        fundingVouts: list[int] = None,
        date: Optional[dt.datetime] = None,
        dates: Optional[list[dt.datetime]] = None,
        extraVins: Optional[list[CMutableTxIn]] = None,
        extraVinsTxinScripts: Optional[list[CScript]] = None,
        extraVouts: Optional[list[CMutableTxOut]] = None,
    ) -> CMutableTransaction:
        ''' compile a claim transaction on a P2SH output '''
        # support both single and multi-input forms
        txIds = fundingTxIds or ([fundingTxId] if fundingTxId else [])
        vouts = fundingVouts or ([fundingVout] if fundingVout is not None else [])
        txins = [
            CMutableTxIn(COutPoint(lx(txId), vout))
            for txId, vout in zip(txIds, vouts)
        ] + (extraVins or [])
        txouts = (
            self._compileCurrencyOutputs(address=address, sats=currencySats - feeOverride)
            if currencySats > 0 else []
        ) + (
            self._compileSatoriOutputs({address: satoriSats})
            if satoriSats > 0 else []
        ) + (extraVouts or [])
        tx = CMutableTransaction(txins, txouts)
        # handle dates (multi-input) or single date
        dateList = dates or ([date] if date else [])
        for i, d in enumerate(dateList):
            if d:
                lock = EvrmoreWallet._cltvNumberFrom(d)
                if getattr(tx, "nLockTime", 0) < lock:
                    tx.nLockTime = lock
                if getattr(tx, "nVersion", 1) < 2: tx.nVersion = 2
                tx.vin[i].nSequence = 0xFFFFFFFE
        redeemCount = len(txIds)
        for i in range(redeemCount):
            sighash = SignatureHash(redeemScript, tx, i, SIGHASH_ALL)
            sig = self.identity._privateKeyObj.sign(sighash) + bytes([SIGHASH_ALL])
            tx.vin[i].scriptSig = redeemParams(sig=sig) + redeemScript
        for i, (txin, txinScriptPubKey) in enumerate(
            zip(tx.vin, ([None] * redeemCount) + (extraVinsTxinScripts or []))
        ):
            if i < redeemCount:
                continue
            self._signInput(
                tx=tx,
                i=i,
                txin=txin,
                txinScriptPubKey=txinScriptPubKey,
                sighashFlag=SIGHASH_ALL)
        return tx

    def _compileClaimOnP2SHMultiSigStart(
        self,
        toAddress: str,
        currencySats: float=0,
        satoriSats: float=0,
        feeOverride: Optional[int] = None,
        fundingTxIds: list[str] = None,
        fundingVouts: list[int] = None,
        dates: Optional[list[dt.datetime]] = None,
        extraVins: Optional[list[CMutableTxIn]] = None,
        extraVouts: Optional[list[CMutableTxOut]] = None,
    ) -> CMutableTransaction:
        txins = [
            CMutableTxIn(COutPoint(lx(fundingTxId), fundingVout))
            for fundingTxId, fundingVout in zip(fundingTxIds, fundingVouts)
            ] + (extraVins or [])
        txouts = (
            self._compileCurrencyOutputs(address=toAddress, sats=currencySats - feeOverride)
            if currencySats > 0 else []
        ) + (
            self._compileSatoriOutputs({toAddress: satoriSats})
            if satoriSats > 0 else []
        ) + (extraVouts or [])
        tx = CMutableTransaction(txins, txouts)
        for i, date in enumerate(dates or []):
            if date:
                lock = EvrmoreWallet._cltvNumberFrom(date)
                if getattr(tx, "nLockTime", 0) < lock:
                    tx.nLockTime = lock
                if getattr(tx, "nVersion", 1) < 2: tx.nVersion = 2
                tx.vin[i].nSequence = 0xFFFFFFFE
        return tx

    def _compileClaimOnP2SHMultiSigMiddle(
        self,
        tx: CMutableTransaction,
        redeemScript: bytes,
        vinIndex: int = 0,
        sighashFlag: int = SIGHASH_ALL,
    ) -> bytes:
        ''' produces a signature for the input at vinIndex '''
        sighash = SignatureHash(redeemScript, tx, vinIndex, sighashFlag)
        sig = self.identity._privateKeyObj.sign(sighash) + bytes([sighashFlag])
        return sig

    def _compileClaimOnP2SHMultiSigEnd(
        self,
        tx: CMutableTransaction,
        redeemScript: bytes,
        redeemParams: Callable,
        redeemCount: int = 1,
        extraVinsTxinScripts: Optional[list[CScript]] = None,
    ) -> CMutableTransaction:
        ''' assumption: txins[0..redeemCount-1] are the p2sh inputs '''
        for i in range(redeemCount):
            tx.vin[i].scriptSig = redeemParams() + redeemScript
        for i, (txin, txinScriptPubKey) in enumerate(
            zip(tx.vin, ([None] * redeemCount) + (extraVinsTxinScripts or []))
        ):
            if i < redeemCount:
                continue
            self._signInput(
                tx=tx,
                i=i,
                txin=txin,
                txinScriptPubKey=txinScriptPubKey,
                sighashFlag=SIGHASH_ALL)
        assert all(len(bytes(v.scriptSig)) > 0 for v in tx.vin), "unsigned input(s)"
        return tx

    ### p2sh - lock - thunder ################################################################

    def produceThunderChannel(
        self,
        receiver: str,
        sender: str=None,
        blocks: int=None,
        minutes: int=None,
        memo: str=None,
        amount: float=None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
        isCurrency: bool = False,
        isExpiring: bool = False,
    ) -> tuple[str, dict[str, Any]]:
        ''' creates a transaction with multiple currency recipients '''
        if isExpiring:
            from satorilib.wallet.evrmore.scripts.channels.lock import thunderExpiring
            thunderChannelFn = thunderExpiring
        else:
            from satorilib.wallet.evrmore.scripts.channels.lock import thunderChannel
            thunderChannelFn = thunderChannel
        from satorilib.wallet.evrmore.utils.multisig import MultisigUtils
        sender = sender or self.pubkey
        redeemScript = thunderChannelFn(
            sender=sender,
            receiver=receiver,
            blocks=blocks,
            minutes=minutes)
        scriptPayload = {
            'redeem_script': str(redeemScript),
            'redeem_script_hex': redeemScript.hex(),
            'redeem_script_size': len(redeemScript),
            'p2sh_address': self.generateP2SHAddress(redeemScript),
            'amount': amount,
            'function': thunderChannelFn.__name__,
            'funding_txid': None, # added during send
            'funding_vout': None, # added during send
            'currency_sats': None, # added during send
            'satori_sats': None, # added during send
            'original_params': {
                'sender': sender,
                'receiver': receiver,
                'blocks': blocks,
                'minutes': minutes,
                'memo': memo,
                'amount': amount,
                'broadcast': broadcast,
                'feeOverride': feeOverride}}
        timestamp = str(time.time())
        MultisigUtils.saveScripts(f'unsent_scripts-{timestamp}.json', [scriptPayload])
        if isCurrency:
            fn = self.produceThunderChannelCurrencyFromScript
        else:
            fn = self.produceThunderChannelFromScript
        txhash, txid, scriptPayload = fn(
            redeemScript=redeemScript,
            scriptPayload=scriptPayload,
            memo=memo,
            broadcast=broadcast,
            feeOverride=feeOverride)
        if len(scriptPayload['funding_txid']) != 64:
            logging.error(f'produceThunderChannel failed: funding_txid is not 64 characters, {txid}')
        scriptPayload['funding_txhash'] = txhash
        print('scriptPayload:', scriptPayload)
        MultisigUtils.saveScripts(
            f'scripts-{timestamp}-{str(time.time())}.json',
            {scriptPayload["p2sh_address"]: scriptPayload})
        return txid, scriptPayload

    def produceThunderChannelCurrency(
        self,
        receiver: str,
        sender: str=None,
        blocks: int=None,
        minutes: int=None,
        memo: str=None,
        amount: float=None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
    ) -> tuple[str, dict[str, Any]]:
        return self.produceThunderChannel(
            receiver=receiver,
            sender=sender,
            blocks=blocks,
            minutes=minutes,
            memo=memo,
            amount=amount,
            broadcast=broadcast,
            feeOverride=feeOverride,
            isCurrency=True,
            isExpiring=False)

    def produceThunderExpiring(
        self,
        receiver: str,
        sender: str=None,
        blocks: int=None,
        minutes: int=None,
        memo: str=None,
        amount: float=None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
    ) -> tuple[str, dict[str, Any]]:
        return self.produceThunderChannel(
            receiver=receiver,
            sender=sender,
            blocks=blocks,
            minutes=minutes,
            memo=memo,
            amount=amount,
            broadcast=broadcast,
            feeOverride=feeOverride,
            isCurrency=False,
            isExpiring=True)

    def produceThunderExpiringCurrency(
        self,
        receiver: str,
        sender: str=None,
        blocks: int=None,
        minutes: int=None,
        memo: str=None,
        amount: float=None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
    ) -> tuple[str, dict[str, Any]]:
        return self.produceThunderChannel(
            receiver=receiver,
            sender=sender,
            blocks=blocks,
            minutes=minutes,
            memo=memo,
            amount=amount,
            broadcast=broadcast,
            feeOverride=feeOverride,
            isCurrency=True,
            isExpiring=True)

    def produceThunderChannelFromScript(
        self,
        redeemScript: bytes,
        scriptPayload: dict[str, Any],
        memo: str = None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
    ) -> tuple[str, str, dict[str, dict]]:
        '''
        creates a transaction with multiple currency recipients
        funding a thunder channel is pretty much a regular transaction
        plus managment of extra p2sh details
        '''
        amount = scriptPayload['amount']
        address = scriptPayload['p2sh_address']
        if amount <= 0 or not Validate.address(address, self.symbol):
            raise TransactionFailure('produceThunderChannel bad params')
        assumedVout = 0
        scriptPayload['funding_txid'] = None
        scriptPayload['funding_vout'] = assumedVout
        scriptPayload['satori_sats'] = TxUtils.roundSatsDownToDivisibility(
            sats=TxUtils.asSats(amount),
            divisibility=self.divisibility)
        memoCount = 0
        if memo is not None:
            memoCount = 1
        satoriSats = scriptPayload['satori_sats']
        (
            gatheredSatoriUnspents,
            gatheredSatoriSats) = self._gatherSatoriUnspents(satoriSats)
        (
            gatheredCurrencyUnspents,
            gatheredCurrencySats) = self._gatherCurrencyUnspents(
                feeOverride=feeOverride,
                inputCount=len(gatheredSatoriUnspents),
                outputCount=1 + 2 + memoCount)
        txins, txinScripts = self._compileInputs(
            gatheredCurrencyUnspents=gatheredCurrencyUnspents,
            gatheredSatoriUnspents=gatheredSatoriUnspents)
        satoriOuts = self._compileSatoriOutputs({address: satoriSats})
        satoriChangeOut = self._compileSatoriChangeOutput(
            satoriSats=satoriSats,
            gatheredSatoriSats=gatheredSatoriSats)
        fee = feeOverride or TxUtils.estimatedFee(
            inputCount=len(txins),
            outputCount=1 + 2 + memoCount)
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
                scriptPayload['funding_txid'] = funding_txid
                return self._txToHex(tx), funding_txid, scriptPayload
            return self._txToHex(tx), '', scriptPayload
        return self.produceThunderChannelFromScript(
            redeemScript=redeemScript,
            scriptPayload=scriptPayload,
            memo=memo,
            broadcast=broadcast,
            feeOverride=requiredFee)

    def produceThunderChannelCurrencyFromScript(
        self,
        script: dict,
        memo: str=None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
    ) -> tuple[str, str, dict]:
        ''' creates a transaction with multiple currency recipients '''
        amount = script['amount']
        address = script['p2sh_address']
        if amount <= 0 or not Validate.address(address, self.symbol):
            raise TransactionFailure('produceThunderChannelCurrency bad params')
        assumedVout = 0
        script['funding_txid'] = None
        script['funding_vout'] = assumedVout
        script['currency_sats'] = TxUtils.roundSatsDownToDivisibility(
            sats=TxUtils.asSats(amount),
            divisibility=self.divisibility)
        memoCount = 0
        if memo is not None:
            memoCount = 1
        (
            gatheredCurrencyUnspents,
            gatheredCurrencySats) = self._gatherCurrencyUnspents(
                feeOverride=feeOverride,
                sats=script['currency_sats'],
                inputCount=len(gatheredCurrencyUnspents),
                outputCount=1 + 1 + memoCount)
        txins, txinScripts = self._compileInputs(
            gatheredCurrencyUnspents=gatheredCurrencyUnspents)
        currencyOuts = self._compileCurrencyOutputs(address=address, sats=script['currency_sats'])
        fee = feeOverride or TxUtils.estimatedFee(
            inputCount=len(txins),
            outputCount=1 + 1 + memoCount)
        currencyChangeOut = self._compileCurrencyChangeOutput(
            currencySats=script['currency_sats'],
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
                script['funding_txid'] = funding_txid
                return self._txToHex(tx), funding_txid, script
            return self._txToHex(tx), '', script
        return self.produceThunderChannelCurrencyFromScript(
            script=script,
            memo=memo,
            broadcast=broadcast,
            feeOverride=requiredFee)

    def produceThunderExpiringFromScript(
        self,
        script: dict,
        memo: str=None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
    ) -> tuple[str, str, dict]:
        ''' alias for produceThunderChannelFromScript '''
        return self.produceThunderChannelFromScript(
            script=script,
            memo=memo,
            broadcast=broadcast,
            feeOverride=feeOverride)

    def produceThunderExpiringCurrencyFromScript(
        self,
        script: dict,
        memo: str=None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
    ) -> tuple[str, str, dict]:
        ''' alias for produceThunderChannelCurrencyFromScript '''
        return self.produceThunderChannelCurrencyFromScript(
            script=script,
            memo=memo,
            broadcast=broadcast,
            feeOverride=feeOverride)

    ### p2sh - unlock - thunder ################################################################

    def thunderChannelTransaction(
        self,
        toAddress: str,
        lockedAmounts: list[float],
        memo: str=None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
        fundingTxIds: list[str] = None,
        fundingVouts: list[int] = None,
        redeemScript: bytes = None, #'CScript'
        timedRelease: bool = True,
        date: Optional[dt.datetime] = None,
        changeAddress: str = None,
        multisigMap: Optional[dict[str, bytes]] = None, # ordered pubkeys: signatures
    ) -> tuple[str, dict[str, int]]:
        ''' claim locked tokens '''
        if (
            sum(lockedAmounts) <= 0 or
            not Validate.address(toAddress, self.symbol)
        ):
            raise TransactionFailure('SimpleTimeReleaseCurrencyTransaction bad params')
        if multisigMap is None:
            return self.thunderChannelRecallTransaction(
                toAddress=toAddress,
                lockedAmounts=lockedAmounts,
                memo=memo,
                broadcast=broadcast,
                feeOverride=feeOverride,
                fundingTxIds=fundingTxIds,
                fundingVouts=fundingVouts,
                redeemScript=redeemScript,
                timedRelease=timedRelease,
                date=date)
        return self.thunderChannelMultisigTransactionStart(
                toAddress=toAddress,
                changeAddress=changeAddress,
                lockedAmounts=lockedAmounts,
                memo=memo,
                feeOverride=feeOverride,
                fundingTxIds=fundingTxIds,
                fundingVouts=fundingVouts,
                date=date)

    def thunderChannelRecallTransaction(
        self,
        toAddress: str,
        lockedAmounts: list[float],
        memo: str=None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
        fundingTxIds: list[str] = None,
        fundingVouts: list[int] = None,
        redeemScript: bytes = None, #'CScript'
        timedRelease: int = 3,
        date: Optional[dt.datetime] = None,
    ) -> tuple[str, dict[str, int]]:
        '''
        claim locked tokens
        note:
            rather than tracking txids and vouts manually,
            use electrumx and derive the dates from the
            tx date + redeemscript locktime
        '''
        from satorilib.wallet.evrmore.scripts.mining import unlock
        lockedAmount = sum(lockedAmounts)
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
            gatheredCurrencyUnspents=gatheredCurrencyUnspents)
        currencyChangeOut = self._compileCurrencyChangeOutput(
            gatheredCurrencySats=gatheredCurrencySats,
            fee=fee)
        memoOut = self._compileMemoOutput(memo)
        tx = self._compileClaimOnP2SH(
            redeemScript=redeemScript,
            redeemParams=redeemParams,
            address=toAddress,
            satoriSats=satoriSats,
            feeOverride=fee,
            fundingTxIds=fundingTxIds,
            fundingVouts=fundingVouts,
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
        return self.thunderChannelRecallTransaction(
            toAddress=toAddress,
            lockedAmounts=lockedAmounts,
            memo=memo,
            broadcast=broadcast,
            fundingTxIds=fundingTxIds,
            fundingVouts=fundingVouts,
            redeemScript=redeemScript,
            timedRelease=timedRelease,
            date=date,
            feeOverride=requiredFee)

    def thunderChannelMultisigTransactionStart(
        self,
        toAddress: str,
        changeAddress: str,
        lockedAmounts: list[float],
        sendAmount: float,
        memo: str=None,
        feeOverride: Optional[int] = None,
        fundingTxIds: list[str] = None,
        fundingVouts: list[int] = None,
    ) -> tuple[str, dict[str, int]]:
        '''
        issue locked tokens from the channel to the receiver
        handle change back to the channel
        possible simplification: always consume everything in the channel
        possible optimization: consume only what is needed, started with the most recent.
        '''
        lockedAmount = sum(lockedAmounts)
        if lockedAmount < sendAmount:
            raise TransactionFailure('sendAmount is greater than lockedAmount')
        satoriSats = TxUtils.roundSatsDownToDivisibility(
            sats=TxUtils.asSats(sendAmount),
            divisibility=self.divisibility)
        fee = feeOverride or TxUtils.defaultFee*4
        (
            gatheredCurrencyUnspents,
            gatheredCurrencySats) = self._gatherCurrencyUnspents(
                feeOverride=fee)
        txins, txinScripts = self._compileInputs(
            gatheredCurrencyUnspents=gatheredCurrencyUnspents)
        satoriChangeOut = self._compileSatoriChangeOutput(
            changeAddress=changeAddress,
            satoriSats=satoriSats,
            gatheredSatoriSats=lockedAmount)
        currencyChangeOut = self._compileCurrencyChangeOutput(
            gatheredCurrencySats=gatheredCurrencySats,
            fee=fee)
        memoOut = self._compileMemoOutput(memo)
        tx = self._compileClaimOnP2SHMultiSigStart(
            toAddress=toAddress,
            satoriSats=satoriSats,
            feeOverride=fee,
            fundingTxIds=fundingTxIds,
            fundingVouts=fundingVouts,
            extraVins=txins,
            extraVouts=(
                [currencyChangeOut] if currencyChangeOut else []
            ) + (
                [satoriChangeOut] if satoriChangeOut else []
            ) + (
                [memoOut] if memoOut else []))
        return tx, txinScripts

    def thunderChannelMultisigTransactionMiddle(
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
            **({sighashFlag: sighashFlag} if sighashFlag else {}))

    def thunderChannelMultisigTransactionEnd(
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

    def thunderChannelCurrencyTransaction(
        self,
        address: str,
        lockedAmount: float,
        memo: str=None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
        fundingTxIds: list[str] = None,
        fundingVouts: list[int] = None,
        dates: Optional[list[dt.datetime]] = None,
        redeemScript: bytes = None, #'CScript'
        timedRelease: bool = True,
        changeAddress: str = None,
        multisigMap: Optional[dict[str, bytes]] = None, # ordered pubkeys: signatures
    ) -> tuple[str, dict[str, int]]:
        ''' claim locked tokens '''
        if (
            lockedAmount <= 0 or
            not Validate.address(address, self.symbol)
        ):
            raise TransactionFailure('SimpleTimeReleaseCurrencyTransaction bad params')
        if multisigMap is None:
            return self.thunderChannelRecallCurrencyTransaction(
                address=address,
                lockedAmount=lockedAmount,
                memo=memo,
                broadcast=broadcast,
                feeOverride=feeOverride,
                fundingTxIds=fundingTxIds,
                fundingVouts=fundingVouts,
                redeemScript=redeemScript,
                timedRelease=timedRelease,
                dates=dates)
        return self.thunderChannelMultisigCurrencyTransactionStart(
                address=address,
                lockedAmount=lockedAmount,
                memo=memo,
                feeOverride=feeOverride,
                fundingTxIds=fundingTxIds,
                fundingVouts=fundingVouts,
                changeAddress=changeAddress)

    def thunderChannelRecallCurrencyTransaction(
        self,
        address: str,
        lockedAmount: float,
        memo: str=None,
        broadcast: bool = True,
        feeOverride: Optional[int] = None,
        fundingTxIds: list[str] = None,
        fundingVouts: list[int] = None,
        dates: Optional[list[dt.datetime]] = None,
        redeemScript: bytes = None, #'CScript'
        timedRelease: int = 3,
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
            fundingTxIds=fundingTxIds,
            fundingVouts=fundingVouts,
            dates=dates,
            extraVouts=[memoOut] if memoOut else [])
        requiredFee = TxUtils.getTxFee(self._txToHex(tx), TxUtils.feeRate)
        print('estimated fee:', fee, 'actual fee:', requiredFee)
        if requiredFee * 0.99 < fee < requiredFee * 1.25:
            if broadcast:
                return self.broadcast(self._txToHex(tx))
            return tx.serialize().hex()
        return self.thunderChannelRecallCurrencyTransaction(
            address=address,
            lockedAmount=lockedAmount,
            memo=memo,
            broadcast=broadcast,
            fundingTxIds=fundingTxIds,
            fundingVouts=fundingVouts,
            redeemScript=redeemScript,
            timedRelease=timedRelease,
            dates=dates,
            feeOverride=requiredFee)

    def thunderChannelMultisigCurrencyTransactionStart(
        self,
        toAddress: str,
        changeAddress: str,
        lockedAmounts: list[float],
        sendAmount: float,
        memo: str=None,
        feeOverride: Optional[int] = None,
        fundingTxIds: list[str] = None,
        fundingVouts: list[int] = None,
    ) -> tuple[str, dict[str, int]]:
        ''' claim locked tokens '''
        lockedAmount = sum(lockedAmounts)
        if lockedAmount < sendAmount:
            raise TransactionFailure('sendAmount is greater than lockedAmount')
        currencySats = TxUtils.roundSatsDownToDivisibility(
            sats=TxUtils.asSats(sendAmount),
            divisibility=self.divisibility)
        fee = feeOverride or TxUtils.defaultFee*4
        currencyChangeOut = self._compileCurrencyChangeOutput(
            currencySats=currencySats,
            gatheredCurrencySats=lockedAmount,
            fee=fee)
        memoOut = self._compileMemoOutput(memo)
        tx = self._compileClaimOnP2SHMultiSigStart(
            toAddress=toAddress,
            currencySats=currencySats,
            feeOverride=fee,
            fundingTxIds=fundingTxIds,
            fundingVouts=fundingVouts,
            extraVouts=(
                [currencyChangeOut] if currencyChangeOut else []
            ) + (
                [memoOut] if memoOut else []))
        return tx

    def thunderChannelMultisigCurrencyTransactionMiddle(
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
            **({sighashFlag: sighashFlag} if sighashFlag else {}))

    def thunderChannelMultisigCurrencyTransactionEnd(
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
