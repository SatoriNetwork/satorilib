"""Data models for Satori Nostr library.

Models for datastream pub/sub with micropayments over Nostr.
"""
import hashlib
import json
import time
import uuid
from dataclasses import dataclass, asdict, fields
from typing import Any


@dataclass
class DatastreamMetadata:
    """Public metadata about a datastream.

    Published as kind 34600 event (plaintext, discoverable).
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
    approval_required: bool = False  # If True, subscribers must request and receive approval before receiving data
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

    def is_likely_active(self, last_observation_time: int, max_staleness_multiplier: float = 1.5) -> bool:
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
            >>> metadata.is_likely_active(last_obs_time)  # True (within 1.5 * 3600 = 5400 seconds)
        """
        now = int(time.time())
        time_since_update = now - last_observation_time

        if self.cadence_seconds is None or self.cadence_seconds <= 0:
            # No cadence: always considered active
            return True
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

    Published as kind 34601 event (parameterized replaceable, relay keeps latest per stream).
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

    Published as kind 34602 event (plaintext, for accountability).
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

    Published as kind 34603 event (encrypted DM to provider).
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
class ChannelCommitment:
    """A partial transaction published by the buyer (sender) for the seller to complete.

    Published as kind 34604 event (parameterized replaceable, d=p2sh_address).
    The relay keeps only the latest commitment per channel, replacing stale ones.
    Tagged with receiver_pubkey so the push delivery goes to the right party.

    Mirrors the Commitment object in Mantra's channel relay service.
    """
    p2sh_address: str           # Channel identifier (P2SH address of the 2-of-2 script)
    sender_pubkey: str          # Buyer's public key (hex)
    receiver_pubkey: str        # Seller's public key (hex)
    partial_tx_hex: str         # Half-signed transaction (hex-encoded)
    sender_sigs: list[str]      # Hex-encoded signatures, one per P2SH input
    pay_amount_sats: int        # Amount being paid to receiver this round
    remainder_sats: int         # Change back to the channel (for the next round)
    fee: int                    # Transaction fee in sats
    timestamp: int              # Unix timestamp when published
    stream_name: str = ''       # Which stream this payment is for

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ChannelCommitment":
        # Tolerant to unknown keys so a producer adding fields (e.g. an
        # unmerged fetch_kind/fetch_t1/fetch_t2 experiment) doesn't break
        # consumers that haven't been upgraded yet.
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> "ChannelCommitment":
        """Deserialize from JSON string."""
        return cls.from_dict(json.loads(json_str))


@dataclass
class InboundCommitment:
    """A received channel commitment after parsing."""
    commitment: ChannelCommitment   # The commitment data
    event_id: str                   # Nostr event ID
    raw_event: dict[str, Any] | None = None  # Optional raw Nostr event


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


@dataclass
class ChannelOpen:
    """Channel open announcement published by the sender to inform the receiver.

    Published as kind 34605 event (parameterized replaceable, d=p2sh_address).
    Tagged with receiver_nostr_pubkey so Nostr pushes it to the right party.
    The receiver saves this to their DB so they can process future commitments.
    """
    p2sh_address: str       # Channel identifier
    sender_pubkey: str      # Sender's EVR wallet pubkey (hex)
    receiver_pubkey: str    # Receiver's EVR wallet pubkey (hex)
    redeem_script: str      # Hex-encoded P2SH redeem script
    funding_txid: str       # Funding transaction ID
    funding_vout: int       # Funding output index
    locked_sats: int        # Amount locked in the channel
    blocks: int | None      # CSV timeout in blocks (mutually exclusive with minutes)
    minutes: float | None   # CSV timeout in minutes (mutually exclusive with blocks)
    timestamp: int          # Unix timestamp when opened
    sender_nostr_pubkey: str = ''  # Sender's Nostr pubkey so receiver can notify them

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ChannelOpen":
        return cls(**data)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> "ChannelOpen":
        return cls.from_dict(json.loads(json_str))


@dataclass
class InboundChannelOpen:
    """A received channel open announcement after parsing."""
    channel_open: ChannelOpen
    event_id: str
    raw_event: dict[str, Any] | None = None


@dataclass
class ChannelSettlement:
    """Notification from receiver to sender after a channel claim.

    Published as kind 34606 event (parameterized replaceable, d=p2sh_address).
    Tagged with sender_nostr_pubkey so Nostr pushes it to the sender.
    Contains the new P2SH funding UTXO so sender can continue paying.
    If new_funding_vout == -1, the channel was fully drained (no change output).
    """
    p2sh_address: str       # Channel identifier
    claim_txid: str         # The broadcast claim tx
    new_funding_vout: int   # Output index of the P2SH change (-1 if fully drained)
    new_locked_sats: int    # New UTXO value (0 if fully drained)
    timestamp: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ChannelSettlement":
        return cls(**data)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> "ChannelSettlement":
        return cls.from_dict(json.loads(json_str))


@dataclass
class InboundChannelSettlement:
    """A received channel settlement notification after parsing."""
    settlement: ChannelSettlement
    event_id: str
    raw_event: dict[str, Any] | None = None


@dataclass
class CompetitionAnnouncement:
    """Public announcement of a prediction competition hosted on a data stream.

    Published as kind 34607 event (parameterized replaceable,
    d=stream_name:stream_provider_pubkey:host_pubkey).

    Re-publishing replaces the previous announcement. Setting active=False
    (or publishing empty content) closes the competition.

    pay_per_obs_sats is the total the host promises to pay per observation,
    distributed across predictors however the host chooses. paid_predictors
    and competing_predictors are intent metadata — social signals, not
    enforced constraints.
    """
    stream_name: str
    stream_provider_pubkey: str
    host_pubkey: str
    pay_per_obs_sats: int
    paid_predictors: int
    competing_predictors: int
    scoring_metric: str
    horizon: int
    active: bool
    timestamp: int
    scoring_params: dict[str, Any] | None = None

    def __post_init__(self):
        if self.scoring_params is None:
            self.scoring_params = {}

    def d_tag(self) -> str:
        return f'{self.stream_name}:{self.stream_provider_pubkey}:{self.host_pubkey}'

    def close(self) -> 'CompetitionAnnouncement':
        import dataclasses
        return dataclasses.replace(
            self, active=False, timestamp=self.timestamp + 1)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'CompetitionAnnouncement':
        return cls(**data)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> 'CompetitionAnnouncement':
        return cls.from_dict(json.loads(json_str))


@dataclass
class PredictionSubmission:
    """A prediction sent from a predictor to a competition host.

    Sent as kind 34608 encrypted DM (NIP-04) directly to the host.
    Not broadcast publicly — only the host can read it.

    The stream is uniquely identified by (stream_name, stream_provider_pubkey).
    seq_num identifies which upcoming observation this predicts.
    """
    stream_name: str
    stream_provider_pubkey: str
    predictor_pubkey: str
    predictor_wallet_pubkey: str
    seq_num: int
    predicted_value: float
    timestamp: int

    def stream_key(self) -> str:
        return f'{self.stream_name}:{self.stream_provider_pubkey}'

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'PredictionSubmission':
        return cls(**data)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> 'PredictionSubmission':
        return cls.from_dict(json.loads(json_str))


@dataclass
class InboundPrediction:
    """A received prediction submission after parsing."""
    prediction: PredictionSubmission
    event_id: str
    raw_event: dict[str, Any] | None = None


@dataclass
class AccessRequest:
    """A request from a subscriber to access an approval-gated stream.

    Sent as kind 34609 encrypted DM (NIP-04) directly to the producer.
    Not broadcast publicly — only the producer can read it.
    """
    stream_name: str
    requester_pubkey: str        # Subscriber requesting access (hex)
    producer_pubkey: str         # Stream producer (hex)
    message: str = ''            # Optional message from requester
    timestamp: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'AccessRequest':
        return cls(**data)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> 'AccessRequest':
        return cls.from_dict(json.loads(json_str))


@dataclass
class InboundAccessRequest:
    """A received access request after decryption."""
    access_request: AccessRequest
    event_id: str
    raw_event: dict[str, Any] | None = None


# Event kind constants
KIND_DATASTREAM_ANNOUNCE = 34600    # Datastream metadata announcement (parameterized replaceable, d=stream_name)
KIND_DATASTREAM_DATA = 34601        # Observation data (parameterized replaceable, d=stream_name — relay keeps latest per stream)
KIND_SUBSCRIPTION_ANNOUNCE = 34602  # Subscription announcement (parameterized replaceable, d=stream_name)
KIND_PAYMENT = 34603                # Payment notification (parameterized replaceable, d=stream_name)
KIND_CHANNEL_COMMITMENT = 34604     # Channel commitment (parameterized replaceable, d=p2sh_address — relay keeps latest per channel)
KIND_CHANNEL_OPEN = 34605           # Channel open announcement (parameterized replaceable, d=p2sh_address — informs receiver)
KIND_CHANNEL_SETTLED = 34606        # Channel settlement notification (parameterized replaceable, d=p2sh_address — informs sender)
KIND_COMPETITION_ANNOUNCE = 34607   # Competition announcement (parameterized replaceable, d=stream_name:provider_pubkey:host_pubkey)
KIND_PREDICTION = 34608             # Prediction submission (encrypted DM from predictor to host)
KIND_ACCESS_REQUEST = 34609         # Access request (encrypted DM from subscriber to producer for approval-gated streams)


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
