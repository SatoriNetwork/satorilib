"""Relay connection management with failover and backoff.

Manages a single active relay connection with automatic failover
to the next relay on failure, using exponential backoff to avoid
hammering failed relays.
"""
import asyncio
from typing import Optional
from nostr_sdk import Client, RelayUrl


class RelayError(Exception):
    """Raised when relay operations fail."""

    pass


class RelayManager:
    """Manages single active relay connection with failover.

    Maintains a list of relay URLs and connects to one at a time.
    On failure, automatically fails over to the next relay using
    round-robin selection with exponential backoff.

    Args:
        relay_urls: List of relay WebSocket URLs
        soft_timeout_ms: Timeout for soft failover (default: 8000ms)
        max_backoff_s: Maximum backoff delay in seconds (default: 30s)

    Example:
        >>> manager = RelayManager(
        ...     ["wss://relay1.com", "wss://relay2.com"],
        ...     soft_timeout_ms=5000
        ... )
        >>> await manager.connect()
        >>> print(manager.get_active_relay())
        'wss://relay1.com'
        >>> await manager.failover()
        >>> print(manager.get_active_relay())
        'wss://relay2.com'
    """

    def __init__(
        self,
        relay_urls: list[str],
        soft_timeout_ms: int = 8000,
        max_backoff_s: float = 30.0,
    ):
        """Initialize relay manager.

        Args:
            relay_urls: List of relay URLs (must have at least one)
            soft_timeout_ms: Timeout before triggering soft failover
            max_backoff_s: Maximum exponential backoff delay

        Raises:
            RelayError: If relay_urls is empty
        """
        if not relay_urls:
            raise RelayError("Must provide at least one relay URL")

        self.relay_urls = relay_urls
        self.soft_timeout_ms = soft_timeout_ms
        self.max_backoff_s = max_backoff_s

        # Internal state
        self._client: Optional[Client] = None
        self._current_index = 0
        self._current_relay_url: Optional[str] = None
        self._connected = False

        # Backoff tracking: relay_url -> failure_count
        self._failure_counts: dict[str, int] = {}

    def get_active_relay(self) -> Optional[str]:
        """Get the currently active relay URL.

        Returns:
            Current relay URL or None if not connected
        """
        return self._current_relay_url

    def is_connected(self) -> bool:
        """Check if currently connected to a relay.

        Returns:
            True if connected, False otherwise
        """
        return self._connected

    async def connect(self) -> None:
        """Connect to the current relay.

        Connects to the relay at the current index position.
        If already connected, disconnects first.

        Raises:
            RelayError: If connection fails
        """
        # Disconnect if already connected
        if self._connected:
            await self.disconnect()

        # Create new client
        self._client = Client()

        # Get current relay URL
        relay_url_str = self.relay_urls[self._current_index]

        try:
            # Parse URL
            relay_url = RelayUrl.parse(relay_url_str)

            # Add and connect to relay
            await self._client.add_relay(relay_url)
            await self._client.connect_relay(relay_url)

            # Mark as connected
            self._connected = True
            self._current_relay_url = relay_url_str

            # Record success (resets backoff)
            self._record_success(relay_url_str)

        except Exception as e:
            self._connected = False
            self._current_relay_url = None
            self._record_failure(relay_url_str)
            raise RelayError(f"Failed to connect to {relay_url_str}: {e}") from e

    async def disconnect(self) -> None:
        """Disconnect from the current relay.

        Closes the connection and cleans up resources.
        Safe to call even if not connected.
        """
        if self._client:
            try:
                await self._client.disconnect()
            except Exception:
                # Ignore disconnect errors
                pass
            finally:
                self._client = None

        self._connected = False
        self._current_relay_url = None

    async def failover(self) -> None:
        """Fail over to the next relay.

        Disconnects from current relay and connects to the next one
        in the list (round-robin). Respects backoff delays for previously
        failed relays.

        Raises:
            RelayError: If failover connection fails
        """
        # Move to next relay (round-robin)
        self._current_index = (self._current_index + 1) % len(self.relay_urls)

        # Get backoff delay for next relay
        next_relay = self.relay_urls[self._current_index]
        backoff_delay = self._get_backoff_delay(next_relay)

        # Wait for backoff if needed
        if backoff_delay > 0:
            await asyncio.sleep(backoff_delay)

        # Connect to new relay
        await self.connect()

    def _get_backoff_delay(self, relay_url: str) -> float:
        """Calculate exponential backoff delay for a relay.

        Uses formula: min(2^failures, max_backoff_s)

        Backoff progression:
        - 0 failures: 1s
        - 1 failure: 2s
        - 2 failures: 4s
        - 3 failures: 8s
        - 4 failures: 16s
        - 5+ failures: 32s (capped at max_backoff_s)

        Args:
            relay_url: Relay URL to check

        Returns:
            Backoff delay in seconds
        """
        failures = self._failure_counts.get(relay_url, 0)

        # Exponential backoff: 2^failures, capped at max
        delay = min(2 ** failures, self.max_backoff_s)
        return delay

    def _record_failure(self, relay_url: str) -> None:
        """Record a connection failure for backoff tracking.

        Args:
            relay_url: Relay URL that failed
        """
        current = self._failure_counts.get(relay_url, 0)
        self._failure_counts[relay_url] = current + 1

    def _record_success(self, relay_url: str) -> None:
        """Record a successful connection (resets backoff).

        Args:
            relay_url: Relay URL that succeeded
        """
        self._failure_counts[relay_url] = 0
