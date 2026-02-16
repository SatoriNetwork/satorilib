"""Data models for Satori Nostr library.

Models for datastream pub/sub with micropayments over Nostr.
"""
import hashlib
import json
import time
import uuid
from dataclasses import dataclass, asdict
from typing import Any


@dataclass
class DatastreamMetadata:
    """Public metadata about a datastream.

    Published as kind 30100 event (plaintext, discoverable).
    """
    stream_name: str         # Human-readable unique name (e.g., "bitcoin-price", "weather-nyc")
    nostr_pubkey: str        # Provider's Nostr public key (hex) - signs events, enforces uniqueness
    name: str                # Human-readable display name
    description: str         # What this stream provides
    encrypted: bool          # Is data encrypted? (True for paid streams)
    price_per_obs: int       # Price in satoshis per observation (0 = free)
    created_at: int          # Unix timestamp when stream was first created
    cadence_seconds: int | None  # Expected seconds between observations (None = irregular)
    tags: list[str]          # Searchable tags (e.g., ["bitcoin", "price", "usd"])
    metadata: dict[str, Any] | None = None  # Optional: source info, lineage, wallet pubkey, etc.

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DatastreamMetadata":
        """Deserialize from dictionary."""
        return cls(**data)

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> "DatastreamMetadata":
        """Deserialize from JSON string."""
        return cls.from_dict(json.loads(json_str))

    def is_likely_active(self, last_observation_time: int, max_staleness_multiplier: float = 2.0) -> bool:
        """Check if stream appears to be actively publishing.

        Args:
            last_observation_time: Unix timestamp of the last observation (from Nostr event)
            max_staleness_multiplier: How many cadence periods before considering stale
                                      (2.0 = allow up to 2x expected delay)

        Returns:
            True if stream appears active based on last observation timestamp

        Example:
            >>> # Stream publishes hourly, last observation 1.5 hours ago
            >>> last_obs_time = int(time.time()) - 5400  # 1.5 hours ago
            >>> metadata.cadence_seconds = 3600
            >>> metadata.is_likely_active(last_obs_time)  # True (within 2 * 3600 = 7200 seconds)
        """
        now = int(time.time())
        time_since_update = now - last_observation_time

        if self.cadence_seconds is None:
            # Irregular cadence: consider active if updated in last 24 hours
            return time_since_update < 86400
        else:
            # Regular cadence: check against expected cadence with tolerance
            max_delay = self.cadence_seconds * max_staleness_multiplier
            return time_since_update < max_delay

    @property
    def uuid(self) -> str:
        """Generate deterministic UUID from nostr_pubkey and stream_name.

        Uses UUID v5 (SHA-1 namespace) to create a reproducible identifier
        from the combination of Nostr publisher identity and stream name.

        The UUID is computed on-demand and not stored, ensuring:
        - Same (nostr_pubkey, stream_name) always produces same UUID
        - Compatibility with systems expecting UUIDs (databases, APIs)
        - No storage overhead

        Returns:
            UUID string in standard format (e.g., "550e8400-e29b-41d4-a716-446655440000")

        Example:
            >>> metadata = DatastreamMetadata(
            ...     stream_name="bitcoin-price",
            ...     nostr_pubkey="abc123",
            ...     # ... other fields
            ... )
            >>> uuid1 = metadata.uuid
            >>> uuid2 = metadata.uuid
            >>> assert uuid1 == uuid2  # Deterministic - always same
        """
        namespace = uuid.NAMESPACE_DNS
        identifier = f"{self.nostr_pubkey}:{self.stream_name}"
        return str(uuid.uuid5(namespace, identifier))


@dataclass
class DatastreamObservation:
    """A single data point in a datastream.

    Published as kind 30101 event (encrypted DM to each paid subscriber).
    """
    stream_name: str         # Which stream this belongs to
    timestamp: int           # Observation time (Unix timestamp)
    value: Any               # The actual data (dict, number, string, list, etc.)
    seq_num: int             # Sequence number for ordering and payment tracking

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "stream_name": self.stream_name,
            "timestamp": self.timestamp,
            "value": self.value,
            "seq_num": self.seq_num,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DatastreamObservation":
        """Deserialize from dictionary."""
        return cls(
            stream_name=data["stream_name"],
            timestamp=data["timestamp"],
            value=data["value"],
            seq_num=data["seq_num"],
        )

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> "DatastreamObservation":
        """Deserialize from JSON string."""
        return cls.from_dict(json.loads(json_str))


@dataclass
class SubscriptionAnnouncement:
    """Public announcement of a datastream subscription.

    Published as kind 30102 event (plaintext, for accountability).
    Lets everyone see who is subscribing to what streams.
    """
    subscriber_pubkey: str   # Who is subscribing (hex)
    stream_name: str         # What they're subscribing to
    nostr_pubkey: str        # Who provides the stream (hex)
    timestamp: int           # When subscription started (Unix timestamp)
    payment_channel: str | None = None  # Optional: Lightning channel address

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SubscriptionAnnouncement":
        """Deserialize from dictionary."""
        return cls(**data)

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> "SubscriptionAnnouncement":
        """Deserialize from JSON string."""
        return cls.from_dict(json.loads(json_str))


@dataclass
class PaymentNotification:
    """Payment notification from subscriber to provider.

    Published as kind 30103 event (encrypted DM to provider).
    Public metadata visible: stream_name, seq_num, timestamp
    Private: amount (optional), transaction details
    """
    from_pubkey: str         # Subscriber's public key (hex)
    to_pubkey: str           # Provider's public key (hex)
    stream_name: str         # What stream this payment is for
    seq_num: int             # Which observation this payment covers
    amount_sats: int         # Payment amount in satoshis
    timestamp: int           # When payment was sent (Unix timestamp)
    tx_id: str | None = None # Optional: Lightning transaction/payment proof

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PaymentNotification":
        """Deserialize from dictionary."""
        return cls(**data)

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> "PaymentNotification":
        """Deserialize from JSON string."""
        return cls.from_dict(json.loads(json_str))


@dataclass
class InboundObservation:
    """A received observation after decryption and parsing."""
    stream_name: str                    # Which stream it's from
    nostr_pubkey: str                   # Provider's public key (hex)
    observation: DatastreamObservation  # The actual observation data
    event_id: str                       # Nostr event ID
    raw_event: dict[str, Any] | None = None  # Optional raw Nostr event


@dataclass
class InboundPayment:
    """A received payment notification after decryption."""
    payment: PaymentNotification        # The payment data
    event_id: str                       # Nostr event ID
    raw_event: dict[str, Any] | None = None  # Optional raw Nostr event


@dataclass
class SatoriNostrConfig:
    """Configuration for SatoriNostr client."""

    # Required fields
    keys: str                           # nsec (bech32) or hex private key
    relay_urls: list[str]               # Relay URLs to connect to

    # Optional fields with defaults
    active_relay_timeout_ms: int = 8000  # Failover trigger timeout
    dedupe_db_path: str | None = None    # SQLite path for dedupe (None = in-memory)


# Event kind constants
KIND_DATASTREAM_ANNOUNCE = 30100    # Datastream metadata announcement
KIND_DATASTREAM_DATA = 30101        # Observation data (encrypted DM)
KIND_SUBSCRIPTION_ANNOUNCE = 30102  # Subscription announcement
KIND_PAYMENT = 30103                # Payment notification (encrypted DM)


# Standard cadence values (in seconds)
CADENCE_REALTIME = 1       # Every second (high-frequency trading, sensors)
CADENCE_MINUTE = 60        # Every minute (active monitoring)
CADENCE_5MIN = 300         # Every 5 minutes (frequent updates)
CADENCE_HOURLY = 3600      # Every hour (recommended for most use cases)
CADENCE_DAILY = 86400      # Every day (sparse data, summaries)
CADENCE_WEEKLY = 604800    # Every week (reports, aggregations)
CADENCE_IRREGULAR = None   # No fixed schedule (event-driven, news, alerts)


def compute_stream_topic_tag(stream_name: str) -> str:
    """Compute a standardized topic tag for a datastream.

    Used for filtering and discovery in Nostr relay queries.

    Args:
        stream_name: Stream name (e.g., "btc-price-usd")

    Returns:
        Topic tag string (e.g., "satori:stream:btc-price-usd")
    """
    return f"satori:stream:{stream_name}"
