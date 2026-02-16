"""
Evermore RPC Client v2 - Based on ElectrumX daemon.py

Adapted from ElectrumX's battle-tested daemon RPC client:
  Original: electrumx/server/daemon.py
  Copyright (c) 2016-2017, Neil Booth
  Adapted: Async→Sync conversion for synchronous use

Provides:
- UTXO discovery (scantxoutset, listunspent)
- Transaction broadcasting
- Multi-node failover with exponential backoff
- Robust error handling from production ElectrumX code
"""
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Union
from decimal import Decimal
import itertools
import json
import time
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import Timeout, ConnectionError as RequestsConnectionError, RequestException


# ============================================================================
# Exception Hierarchy (from ElectrumX + extensions)
# ============================================================================

class RpcClientError(Exception):
    """Base exception for all RPC client errors"""
    def __init__(self, message: str, node_url: str = None, method: str = None, code: int = None):
        super().__init__(message)
        self.message = message
        self.node_url = node_url
        self.method = method
        self.code = code


class DaemonError(RpcClientError):
    """Raised when the daemon returns an error in its results (from ElectrumX)"""
    pass


class WarmingUpError(RpcClientError):
    """Raised when the daemon is warming up - code -28 (from ElectrumX)"""
    pass


class ServiceRefusedError(RpcClientError):
    """Raised when daemon doesn't provide JSON response, only HTTP error (from ElectrumX)"""
    pass


class RpcTransportError(RpcClientError):
    """Network/transport layer errors (timeouts, connection refused)"""
    pass


class RpcAuthError(RpcClientError):
    """Authentication errors"""
    pass


class RpcMethodNotFound(RpcClientError):
    """RPC method not found (-32601)"""
    pass


class RpcError(RpcClientError):
    """Generic RPC error (alias for DaemonError for v1 API compatibility)"""
    pass


class UtxoQueryError(RpcClientError):
    """UTXO discovery failed"""
    pass


class BroadcastRejected(RpcClientError):
    """Transaction broadcast rejected by network"""
    pass


# ============================================================================
# Data Models
# ============================================================================

@dataclass
class UTXO:
    """Unspent transaction output"""
    txid: str
    vout: int
    amount: Decimal
    confirmations: int
    scriptPubKey: str
    address: Optional[str] = None
    spendable: Optional[bool] = None
    solvable: Optional[bool] = None
    safe: Optional[bool] = None
    label: Optional[str] = None
    blockhash: Optional[str] = None


# ============================================================================
# Main Client (adapted from ElectrumX Daemon class)
# ============================================================================

class EvermoreRpcClient:
    """
    Evermore full node RPC client - adapted from ElectrumX daemon.py

    Core patterns from ElectrumX:
    - Exponential backoff retry logic
    - Multi-node failover
    - Robust error handling
    - Batch RPC support

    API:
    - get_unspents(addresses, min_conf, max_conf) -> List[UTXO]
    - broadcast_raw_tx(raw_tx_hex) -> str (txid)
    - get_block_count() -> int
    - ping() -> dict
    - rpc_call(method, params) -> any
    """

    # From ElectrumX: warming up error code
    WARMING_UP = -28
    # ID counter for JSON-RPC (from ElectrumX pattern)
    id_counter = itertools.count()

    def __init__(
        self,
        nodes: List[Dict[str, str]],
        init_retry: float = 0.25,
        max_retry: float = 4.0,
        timeout: float = 30.0,
        utxo_mode: str = "scantxoutset",
        verify_tls: Union[bool, str] = True,
    ):
        """
        Initialize RPC client with ElectrumX retry patterns

        Args:
            nodes: List of node configs with 'url', 'user', 'password'
            init_retry: Initial retry delay in seconds (ElectrumX: 0.25)
            max_retry: Maximum retry delay in seconds (ElectrumX: 4.0)
            timeout: HTTP request timeout
            utxo_mode: "scantxoutset" or "wallet_listunspent"
            verify_tls: TLS verification
        """
        if not nodes:
            raise ValueError("At least one node must be provided")

        self.nodes = nodes
        self.url_index = 0
        self.init_retry = init_retry
        self.max_retry = max_retry
        self.timeout = timeout
        self.utxo_mode = utxo_mode
        self.verify_tls = verify_tls
        self._height = None

        # Validate UTXO mode
        if utxo_mode not in ("scantxoutset", "wallet_listunspent"):
            raise ValueError(f"Invalid utxo_mode: {utxo_mode}")

    def current_url(self):
        """Returns the current daemon URL (from ElectrumX)"""
        return self.nodes[self.url_index]["url"]

    def current_auth(self):
        """Returns current node authentication"""
        node = self.nodes[self.url_index]
        user = node.get("user")
        password = node.get("password")
        if user and password:
            return HTTPBasicAuth(user, password)
        return None

    def logged_url(self):
        """The URL for logging (from ElectrumX pattern)"""
        url = self.current_url()
        # Hide credentials if present in URL
        if "@" in url:
            return url[url.rindex("@") + 1:]
        return url

    def failover(self):
        """
        Fail-over to the next daemon URL (from ElectrumX)

        Returns False if there is only one, otherwise True
        """
        if len(self.nodes) > 1:
            self.url_index = (self.url_index + 1) % len(self.nodes)
            return True
        return False

    def _post_json(self, payload, processor):
        """
        POST JSON-RPC request (adapted from ElectrumX _post_json)

        Args:
            payload: JSON-RPC payload
            processor: Function to process response

        Returns:
            Processed result
        """
        data = json.dumps(payload)
        response = requests.post(
            self.current_url(),
            data=data,
            auth=self.current_auth(),
            timeout=self.timeout,
            verify=self.verify_tls,
            headers={"Content-Type": "application/json"}
        )

        # Check HTTP status code
        if response.status_code == 401 or response.status_code == 403:
            raise RpcAuthError(
                f"Authentication failed: {response.status_code}",
                node_url=self.current_url()
            )

        if response.status_code != 200:
            try:
                error_text = response.text[:200] if hasattr(response, 'text') else ''
            except:
                error_text = ''
            raise RpcTransportError(
                f"HTTP {response.status_code}: {error_text}",
                node_url=self.current_url()
            )

        # Try to parse as JSON
        try:
            return processor(response.json())
        except (ValueError, json.JSONDecodeError, AttributeError):
            # Not JSON - service refused
            try:
                text = response.text.strip() or response.reason
            except (AttributeError, TypeError):
                text = "Service refused"
            raise ServiceRefusedError(text, node_url=self.current_url())

    def _send(self, func, *args):
        """
        Send with retry and failover logic (from ElectrumX _send)

        Handles temporary connection issues with exponential backoff.
        Daemon response errors are raised through DaemonError.
        """
        def log_error(error):
            nonlocal last_error_log, retry
            now = time.monotonic()
            # Log at most once per minute (from ElectrumX)
            if now - last_error_log > 60:
                last_error_log = now
                # In production ElectrumX logs here, we'll store for exception
            # After max retry, attempt failover (from ElectrumX)
            if retry >= self.max_retry and self.failover():
                retry = self.init_retry

        on_good_message = None
        last_error_log = -1000  # Monotonic time starts at 0
        retry = self.init_retry
        last_error = None

        # ElectrumX retries indefinitely, we'll add a reasonable limit
        max_attempts = len(self.nodes) * 10

        for attempt in range(max_attempts):
            try:
                result = func(*args)
                # If we had errors before, note recovery
                if on_good_message:
                    pass  # Connection restored
                return result

            except Timeout:
                last_error = "timeout error"
                log_error(last_error)
                on_good_message = "connection restored"

            except RequestsConnectionError:
                last_error = "connection problem - check your daemon is running"
                log_error(last_error)
                on_good_message = "connection restored"

            except ConnectionResetError:
                last_error = "connection reset"
                log_error(last_error)
                on_good_message = "connection restored"

            except ServiceRefusedError as e:
                last_error = f"daemon service refused: {e.message}"
                log_error(last_error)
                on_good_message = "running normally"

            except WarmingUpError:
                last_error = "daemon starting up, checking blocks"
                log_error(last_error)
                on_good_message = "running normally"

            except (DaemonError, RpcAuthError, RpcMethodNotFound):
                # These should not retry (application errors, not network)
                raise

            except RequestException as e:
                last_error = f"request failed: {e}"
                log_error(last_error)
                on_good_message = "connection restored"

            # Sleep with exponential backoff (from ElectrumX)
            time.sleep(retry)
            retry = max(min(self.max_retry, retry * 2), self.init_retry)

        # All retries exhausted
        raise RpcTransportError(
            f"All retries exhausted. Last error: {last_error}",
            node_url=self.current_url()
        )

    def _send_single(self, method, params=None):
        """
        Send a single request to the daemon (from ElectrumX _send_single)

        Args:
            method: RPC method name
            params: Method parameters

        Returns:
            RPC result
        """
        def processor(result):
            err = result.get("error")
            if not err:
                return result.get("result")

            # Check for warming up (from ElectrumX)
            if err.get("code") == self.WARMING_UP:
                raise WarmingUpError(
                    "Daemon is warming up",
                    node_url=self.current_url(),
                    method=method,
                    code=self.WARMING_UP
                )

            # Check for method not found
            if err.get("code") == -32601:
                raise RpcMethodNotFound(
                    f"Method not found: {method}",
                    node_url=self.current_url(),
                    method=method,
                    code=-32601
                )

            # Check for auth errors
            if err.get("code") in (-1, -5):
                raise RpcAuthError(
                    err.get("message", "Authentication failed"),
                    node_url=self.current_url(),
                    method=method,
                    code=err.get("code")
                )

            # Generic daemon error
            raise DaemonError(
                err.get("message", "Unknown error"),
                node_url=self.current_url(),
                method=method,
                code=err.get("code")
            )

        payload = {
            "method": method,
            "id": next(self.id_counter),
            "jsonrpc": "1.0"
        }
        if params:
            payload["params"] = params

        return self._send(self._post_json, payload, processor)

    def _send_vector(self, method, params_iterable, replace_errs=False):
        """
        Send several requests of the same method (from ElectrumX _send_vector)

        The result will be an array of the same length as params_iterable.
        If replace_errs is true, any item with an error is returned as None.

        Args:
            method: RPC method name
            params_iterable: Iterable of parameter lists
            replace_errs: Replace errors with None instead of raising

        Returns:
            List of results
        """
        def processor(result):
            errs = [item["error"] for item in result if item.get("error")]

            # Check for warming up
            if any(err.get("code") == self.WARMING_UP for err in errs):
                raise WarmingUpError(
                    "Daemon is warming up",
                    node_url=self.current_url(),
                    method=method
                )

            if not errs or replace_errs:
                return [item.get("result") for item in result]

            raise DaemonError(
                f"Batch call errors: {errs}",
                node_url=self.current_url(),
                method=method
            )

        payload = [
            {
                "method": method,
                "params": p,
                "id": next(self.id_counter),
                "jsonrpc": "1.0"
            }
            for p in params_iterable
        ]

        if payload:
            return self._send(self._post_json, payload, processor)
        return []

    def rpc_call(self, method: str, params: List = None) -> Any:
        """
        Low-level RPC call (convenience wrapper)

        Args:
            method: RPC method name
            params: Method parameters

        Returns:
            RPC result
        """
        return self._send_single(method, params)

    def get_block_count(self) -> int:
        """
        Query the daemon for its current height (from ElectrumX height())

        Returns:
            Current block height
        """
        self._height = self._send_single("getblockcount")
        return self._height

    def cached_height(self):
        """
        Return the cached daemon height (from ElectrumX)

        Returns None if not yet queried
        """
        return self._height

    def broadcast_raw_tx(self, raw_tx_hex: str) -> str:
        """
        Broadcast a raw transaction (from ElectrumX broadcast_transaction)

        Args:
            raw_tx_hex: Hex-encoded signed transaction

        Returns:
            Transaction ID
        """
        # Validate hex
        if not self._is_valid_hex(raw_tx_hex):
            raise ValueError(f"Invalid hex string: {raw_tx_hex[:50]}...")

        try:
            return self._send_single("sendrawtransaction", [raw_tx_hex])
        except DaemonError as e:
            # Check for already in mempool
            if "already" in e.message.lower() and "mempool" in e.message.lower():
                return "already-in-mempool"

            # Other errors are broadcast rejections
            raise BroadcastRejected(
                e.message,
                node_url=e.node_url,
                method=e.method,
                code=e.code
            )

    def ping(self) -> Dict[str, Any]:
        """
        Health check - get blockchain info (similar to ElectrumX getnetworkinfo)

        Returns:
            Blockchain info dict
        """
        return self._send_single("getblockchaininfo")

    def _is_valid_hex(self, hex_str: str) -> bool:
        """Validate hex string"""
        if not hex_str:
            return False
        if len(hex_str) % 2 != 0:
            return False
        try:
            int(hex_str, 16)
            return True
        except ValueError:
            return False

    def _build_descriptors(self, addresses: List[str]) -> List[str]:
        """Build descriptors for scantxoutset"""
        # Deduplicate while preserving order
        seen = set()
        unique = []
        for addr in addresses:
            if addr not in seen:
                seen.add(addr)
                unique.append(addr)
        return [f"addr({addr})" for addr in unique]

    def _calculate_confirmations(self, utxo_height: Optional[int], current_height: int) -> int:
        """Calculate confirmations from block heights"""
        if utxo_height is None:
            return 0
        return current_height - utxo_height + 1

    def get_unspents(
        self,
        addresses: Union[str, List[str]],
        min_conf: int = 0,
        max_conf: int = 9999999
    ) -> List[UTXO]:
        """
        Get unspent transaction outputs for given addresses

        Args:
            addresses: Single address or list of addresses
            min_conf: Minimum confirmations
            max_conf: Maximum confirmations

        Returns:
            List of UTXO objects
        """
        if not addresses:
            return []

        if isinstance(addresses, str):
            addresses = [addresses]

        try:
            if self.utxo_mode == "scantxoutset":
                return self._get_unspents_scantxoutset(addresses, min_conf, max_conf)
            else:
                return self._get_unspents_wallet(addresses, min_conf, max_conf)
        except (DaemonError, RpcMethodNotFound) as e:
            raise UtxoQueryError(
                f"UTXO query failed: {e.message}",
                node_url=e.node_url,
                method=e.method,
                code=e.code
            )

    def _get_unspents_scantxoutset(
        self,
        addresses: List[str],
        min_conf: int,
        max_conf: int
    ) -> List[UTXO]:
        """Get unspents using scantxoutset"""
        descriptors = self._build_descriptors(addresses)

        try:
            result = self._send_single("scantxoutset", ["start", descriptors])
        except RpcMethodNotFound:
            raise UtxoQueryError(
                "scantxoutset not supported. Try utxo_mode='wallet_listunspent'",
                method="scantxoutset"
            )

        if not result or not result.get("unspents"):
            return []

        # Get current height
        current_height = self.get_block_count()

        utxos = []
        for unspent in result["unspents"]:
            height = unspent.get("height")
            confirmations = self._calculate_confirmations(height, current_height)

            # Filter by confirmation range
            if confirmations < min_conf or confirmations > max_conf:
                continue

            # Parse amount
            amount_raw = unspent.get("amount", 0)
            if isinstance(amount_raw, str):
                amount = Decimal(amount_raw)
            else:
                amount = Decimal(str(amount_raw))

            utxo = UTXO(
                txid=unspent["txid"],
                vout=unspent["vout"],
                amount=amount,
                confirmations=confirmations,
                scriptPubKey=unspent.get("scriptPubKey", ""),
                address=unspent.get("address"),
                spendable=unspent.get("spendable"),
                solvable=unspent.get("solvable"),
                safe=unspent.get("safe"),
                label=unspent.get("label"),
                blockhash=unspent.get("blockhash"),
            )
            utxos.append(utxo)

        return utxos

    def _get_unspents_wallet(
        self,
        addresses: List[str],
        min_conf: int,
        max_conf: int
    ) -> List[UTXO]:
        """Get unspents using listunspent (requires addresses in wallet)"""
        result = self._send_single("listunspent", [min_conf, max_conf, addresses])

        if not result:
            return []

        utxos = []
        for unspent in result:
            amount_raw = unspent.get("amount", 0)
            if isinstance(amount_raw, str):
                amount = Decimal(amount_raw)
            else:
                amount = Decimal(str(amount_raw))

            utxo = UTXO(
                txid=unspent["txid"],
                vout=unspent["vout"],
                amount=amount,
                confirmations=unspent.get("confirmations", 0),
                scriptPubKey=unspent.get("scriptPubKey", ""),
                address=unspent.get("address"),
                spendable=unspent.get("spendable"),
                solvable=unspent.get("solvable"),
                safe=unspent.get("safe"),
                label=unspent.get("label"),
                blockhash=unspent.get("blockhash"),
            )
            utxos.append(utxo)

        return utxos
