"""Core SatoriNostr client for datastream pub/sub over Nostr.

Provides APIs for:
- Providers: announcing streams, publishing observations, receiving payments
- Subscribers: discovering streams, subscribing, sending payments, receiving data
"""
import asyncio
import time
import json
from typing import Optional, AsyncIterator
from dataclasses import dataclass
from datetime import timedelta

from nostr_sdk import (
    Keys,
    Client,
    Filter,
    Kind,
    EventBuilder,
    Tag,
    PublicKey,
    Event,
    SecretKey,
    Timestamp,
    NostrSigner,
    RelayUrl,
    HandleNotification,
    SingleLetterTag,
    Alphabet,
)

from .models import (
    SatoriNostrConfig,
    DatastreamMetadata,
    DatastreamObservation,
    SubscriptionAnnouncement,
    PaymentNotification,
    ChannelCommitment,
    ChannelOpen,
    ChannelSettlement,
    CompetitionAnnouncement,
    PredictionSubmission,
    InboundPrediction,
    InboundObservation,
    InboundPayment,
    InboundCommitment,
    InboundChannelOpen,
    InboundChannelSettlement,
    AccessRequest,
    InboundAccessRequest,
    KIND_DATASTREAM_ANNOUNCE,
    KIND_DATASTREAM_DATA,
    KIND_SUBSCRIPTION_ANNOUNCE,
    KIND_PAYMENT,
    KIND_CHANNEL_COMMITMENT,
    KIND_CHANNEL_OPEN,
    KIND_CHANNEL_SETTLED,
    KIND_COMPETITION_ANNOUNCE,
    KIND_PREDICTION,
    KIND_ACCESS_REQUEST,
    compute_stream_topic_tag,
)
from .dedupe import DedupeCache
from .encryption import (
    encrypt_observation,
    decrypt_observation,
    encrypt_payment,
    decrypt_payment,
    EncryptionError,
)


@dataclass
class SubscriberState:
    """Tracks state for a single subscriber to a datastream."""
    subscriber_pubkey: str
    stream_name: str
    last_paid_seq: int | None = None  # Last seq_num they paid for
    payment_channel: str | None = None
    subscribed_at: int = 0  # Unix timestamp


class SatoriNostr:
    """Main client for Satori datastream pub/sub over Nostr.

    Provides high-level APIs for datastream providers and subscribers.

    Example (Provider):
        >>> config = SatoriNostrConfig(
        ...     keys="nsec1...",
        ...     relay_urls=["wss://relay.damus.io"]
        ... )
        >>> client = SatoriNostr(config)
        >>> await client.start()
        >>>
        >>> # Announce a datastream
        >>> metadata = DatastreamMetadata(...)
        >>> await client.announce_datastream(metadata)
        >>>
        >>> # Publish observations to paid subscribers
        >>> obs = DatastreamObservation(...)
        >>> await client.publish_observation(obs)
        >>>
        >>> # Monitor payments
        >>> async for payment in client.payments():
        ...     print(f"Payment received: {payment.amount_sats} sats")

    Example (Subscriber):
        >>> # Discover datastreams
        >>> streams = await client.discover_datastreams(tags=["bitcoin"])
        >>>
        >>> # Subscribe to a stream
        >>> await client.subscribe_datastream("btc-price", provider_pubkey)
        >>>
        >>> # Send payment for observation
        >>> await client.send_payment(provider_pubkey, "btc-price", seq_num=42, amount=10)
        >>>
        >>> # Receive observations
        >>> async for obs in client.observations():
        ...     print(obs.observation.value)
    """

    def __init__(self, config: SatoriNostrConfig):
        """Initialize SatoriNostr client.

        Args:
            config: Client configuration with keys and relays
        """
        self.config = config

        # Parse keys
        if config.keys.startswith("nsec"):
            self._keys = Keys.parse(config.keys)
        else:
            # Assume hex private key
            secret_key = SecretKey.parse(config.keys)
            self._keys = Keys(secret_key)

        # Initialize nostr-sdk client
        self._client: Optional[Client] = None

        # State management
        self._running = False
        self._dedupe = DedupeCache()

        # Subscriber tracking (for providers)
        # stream_name -> {subscriber_pubkey -> SubscriberState}
        self._subscribers: dict[str, dict[str, SubscriberState]] = {}

        # Track announced streams (for providers)
        self._announced_streams: dict[str, DatastreamMetadata] = {}

        # Event queues for async iteration
        self._observation_queue: asyncio.Queue[InboundObservation] = asyncio.Queue()
        self._payment_queue: asyncio.Queue[InboundPayment] = asyncio.Queue()
        self._commitment_queue: asyncio.Queue[InboundCommitment] = asyncio.Queue()
        self._channel_open_queue: asyncio.Queue[InboundChannelOpen] = asyncio.Queue()
        self._settlement_queue: asyncio.Queue[InboundChannelSettlement] = asyncio.Queue()
        self._tombstone_queue: asyncio.Queue[str] = asyncio.Queue()  # p2sh_address strings
        self._prediction_queue: asyncio.Queue[InboundPrediction] = asyncio.Queue()
        self._access_request_queue: asyncio.Queue[InboundAccessRequest] = asyncio.Queue()

        # Statistics
        self._stats = {
            "observations_sent": 0,
            "observations_received": 0,
            "payments_sent": 0,
            "payments_received": 0,
            "subscriptions_announced": 0,
            "commitments_sent": 0,
            "commitments_received": 0,
        }

        # Background tasks
        self._listener_task: Optional[asyncio.Task] = None

    def pubkey(self) -> str:
        """Get the client's public key.

        Returns:
            Public key as hex string
        """
        return self._keys.public_key().to_hex()

    def is_running(self) -> bool:
        """Check if client is currently running.

        Returns:
            True if running, False otherwise
        """
        return self._running

    async def start(self) -> None:
        """Start the client and connect to relays.

        Creates relay connections and starts background event processing.

        Raises:
            RuntimeError: If already running
        """
        if self._running:
            raise RuntimeError("Client is already running")

        # Create nostr-sdk client
        self._client = Client(NostrSigner.keys(self._keys))

        # Add relays
        for relay_url in self.config.relay_urls:
            await self._client.add_relay(RelayUrl.parse(relay_url))

        # Connect to relays and wait for connection
        await self._client.connect()
        await self._client.wait_for_connection(timedelta(seconds=5))

        # Start listening for events
        self._listener_task = asyncio.create_task(self._event_listener())

        self._running = True

    async def stop(self) -> None:
        """Stop the client and disconnect from relays.

        Raises:
            RuntimeError: If not running
        """
        if not self._running:
            raise RuntimeError("Client is not running")

        # Cancel listener task
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass

        # Disconnect from relays
        if self._client:
            await self._client.disconnect()

        self._running = False

    # ========================================================================
    # PROVIDER APIs
    # ========================================================================

    async def announce_datastream(self, metadata: DatastreamMetadata, deleted: bool = False) -> str:
        """Announce a datastream (provider).

        Publishes stream metadata as a kind 34600 event (public, discoverable).
        When deleted=True, publishes a tombstone that replaces the original
        announcement and signals to other nodes that this stream is gone.

        Args:
            metadata: Datastream metadata to announce
            deleted: If True, publish a tombstone (status=deleted) event

        Returns:
            Event ID of the announcement

        Raises:
            RuntimeError: If client not running
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")

        # Build tags for discovery
        tags = [
            Tag.parse(["d", metadata.stream_name]),  # Replaceable event identifier
            Tag.parse(["satori", "datastream"]),
        ]

        if deleted:
            tags.append(Tag.parse(["status", "deleted"]))
        else:
            # Add topic tags for discovery
            for tag in metadata.tags:
                tags.append(Tag.parse(["t", tag]))

            # Add stream topic tag
            tags.append(Tag.parse(["stream", compute_stream_topic_tag(metadata.stream_name)]))

            # Add source lineage tags if this stream predicts another
            if metadata.metadata:
                src_stream = metadata.metadata.get('source_stream_name')
                src_pubkey = metadata.metadata.get('source_provider_pubkey')
                if src_stream:
                    tags.append(Tag.parse(["source_stream", src_stream]))
                if src_pubkey:
                    tags.append(Tag.parse(["source_pubkey", src_pubkey]))

            # Add approval-gated tag for private streams
            if metadata.approval_required:
                tags.append(Tag.parse(["approval_required", "true"]))

        # Build event with metadata as content
        builder = EventBuilder(
            Kind(KIND_DATASTREAM_ANNOUNCE),
            metadata.to_json(),
        ).tags(tags)

        # Publish to relays
        output = await self._client.send_event_builder(builder)

        if not deleted:
            # Track announced stream
            self._announced_streams[metadata.stream_name] = metadata

        return output.id.to_hex()

    async def announce_competition(self, competition: CompetitionAnnouncement) -> str:
        """Announce a prediction competition (host).

        Publishes competition metadata as a kind 34607 parameterized replaceable
        event. Re-publishing with the same d-tag replaces the previous announcement.

        Args:
            competition: Competition announcement to publish

        Returns:
            Event ID of the announcement

        Raises:
            RuntimeError: If client not running
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")

        tags = [
            Tag.parse(["d", competition.d_tag()]),
            Tag.parse(["satori", "competition"]),
            Tag.parse(["s", competition.stream_name]),
            Tag.parse(["p", competition.stream_provider_pubkey]),
        ]

        builder = EventBuilder(
            Kind(KIND_COMPETITION_ANNOUNCE),
            competition.to_json(),
        ).tags(tags)

        output = await self._client.send_event_builder(builder)
        return output.id.to_hex()

    async def close_competition(self, competition: CompetitionAnnouncement) -> str:
        """Close a competition by publishing an inactive replacement (host).

        Args:
            competition: The competition to close (active flag will be set False)

        Returns:
            Event ID of the closing event

        Raises:
            RuntimeError: If client not running
        """
        return await self.announce_competition(competition.close())

    async def discover_competitions(
        self,
        stream_name: str | None = None,
        active_only: bool = False,
        limit: int = 100,
    ) -> list[CompetitionAnnouncement]:
        """Discover available prediction competitions (predictor/observer).

        Queries relays for competition announcements (kind 34607).

        Args:
            stream_name: Optional stream name to filter by
            active_only: If True, only return competitions with active=True
            limit: Maximum number of results

        Returns:
            List of CompetitionAnnouncement

        Raises:
            RuntimeError: If client not running
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")

        filter_builder = Filter().kind(Kind(KIND_COMPETITION_ANNOUNCE)).limit(limit)
        if stream_name:
            filter_builder = filter_builder.custom_tag(
                SingleLetterTag.lowercase(Alphabet.S), stream_name)

        events_obj = await self._client.fetch_events(
            filter_builder, timedelta(seconds=10))
        events = events_obj.to_vec()

        # Deduplicate by d-tag keeping the latest event (parameterized replaceable).
        # Use content timestamp so close() (which bumps timestamp by 1) always wins
        # over the original even when both Nostr events share the same created_at second.
        latest: dict[str, tuple[int, CompetitionAnnouncement]] = {}
        for event in events:
            try:
                c = CompetitionAnnouncement.from_json(event.content())
                d = c.d_tag()
                ts = c.timestamp  # content timestamp, not Nostr event created_at
                if d not in latest or ts > latest[d][0]:
                    latest[d] = (ts, c)
            except Exception as e:
                print(f"Error parsing competition announcement: {e}")

        competitions = [c for _, c in latest.values()]
        if active_only:
            competitions = [c for c in competitions if c.active]
        return competitions

    async def submit_prediction(
        self,
        stream_name: str,
        stream_provider_pubkey: str,
        host_pubkey: str,
        seq_num: int,
        predicted_value: float,
        predictor_wallet_pubkey: str,
    ) -> str:
        """Submit a prediction to a competition host (predictor).

        Sends an encrypted DM (kind 34608, NIP-04) directly to the host.
        Not broadcast publicly — only the host can decrypt it.

        Args:
            stream_name: Stream being predicted
            stream_provider_pubkey: Producer of the stream
            host_pubkey: Competition host's Nostr pubkey
            seq_num: Observation sequence number being predicted
            predicted_value: The predictor's prediction
            predictor_wallet_pubkey: Predictor's EVRmore wallet pubkey, so the
                host can open a payment channel and pay them

        Returns:
            Event ID

        Raises:
            RuntimeError: If client not running
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")

        prediction = PredictionSubmission(
            stream_name=stream_name,
            stream_provider_pubkey=stream_provider_pubkey,
            predictor_pubkey=self.pubkey(),
            predictor_wallet_pubkey=predictor_wallet_pubkey,
            seq_num=seq_num,
            predicted_value=predicted_value,
            timestamp=int(time.time()),
        )

        host_pk = PublicKey.parse(host_pubkey)
        from .encryption import encrypt_json
        encrypted = encrypt_json(prediction.to_json(), host_pk, self._keys)

        tags = [
            Tag.parse(["p", host_pubkey]),
            Tag.parse(["s", stream_name]),
            Tag.parse(["provider", stream_provider_pubkey]),
            Tag.parse(["seq", str(seq_num)]),
        ]

        builder = EventBuilder(Kind(KIND_PREDICTION), encrypted).tags(tags)
        output = await self._client.send_event_builder(builder)
        return output.id.to_hex()

    async def incoming_predictions(self) -> AsyncIterator['InboundPrediction']:
        """Receive incoming prediction submissions (host).

        Yields InboundPrediction as predictors submit them.
        """
        while True:
            try:
                pred = await asyncio.wait_for(
                    self._prediction_queue.get(), timeout=1.0)
                yield pred
            except asyncio.TimeoutError:
                continue

    async def publish_observation(
        self,
        observation: DatastreamObservation,
        stream_metadata: DatastreamMetadata
    ) -> list[str]:
        """Publish an observation to all paid subscribers (provider).

        Sends encrypted DMs (kind 34601) to each subscriber who has paid
        for this observation's sequence number.

        Args:
            observation: The observation to publish
            stream_metadata: Metadata about this stream (for pricing info)

        Returns:
            List of event IDs (one per subscriber sent to)

        Raises:
            RuntimeError: If client not running
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")

        stream_name = observation.stream_name
        seq_num = observation.seq_num

        # Get subscribers for this stream
        stream_subscribers = self._subscribers.get(stream_name, {})

        event_ids = []
        obs_json = observation.to_json()

        if stream_metadata.price_per_obs == 0:
            # Free stream: broadcast a single event (not per-subscriber)
            content = obs_json
            if stream_metadata.encrypted:
                # Encrypted broadcast: encrypt with provider's own key
                content = encrypt_observation(
                    obs_json, self._keys.public_key(), self._keys)

            tags = [
                Tag.parse(["d", stream_name]),  # Replaceable: latest obs per stream
                Tag.parse(["stream", stream_name]),
                Tag.parse(["seq", str(seq_num)]),
                Tag.parse(["satori", "observation"]),
            ]

            builder = EventBuilder(
                Kind(KIND_DATASTREAM_DATA),
                content,
            ).tags(tags)

            output = await self._client.send_event_builder(builder)
            event_ids.append(output.id.to_hex())
            self._stats["observations_sent"] += 1
        else:
            # Paid stream: send one event per eligible subscriber with only the
            # value encrypted in content. All other metadata (seq, timestamp,
            # stream_name) goes in plaintext tags. This eliminates the need for
            # a separate heartbeat event — the tags alone carry enough info for
            # marketplace freshness checks — and removes the d-tag collision
            # where heartbeat and encrypted obs shared the same replaceable
            # d-tag, causing the relay to suppress the encrypted delivery.
            value_str = str(observation.value)

            for sub_pubkey, sub_state in stream_subscribers.items():
                paid = (sub_state.last_paid_seq is not None
                        and sub_state.last_paid_seq >= seq_num)
                free = sub_state.last_paid_seq is None

                if not (paid or free):
                    continue

                try:
                    recipient_pubkey = PublicKey.parse(sub_pubkey)
                    encrypted_value = encrypt_observation(
                        value_str, recipient_pubkey, self._keys)

                    tags = [
                        Tag.parse(["d", stream_name]),
                        Tag.parse(["p", sub_pubkey]),
                        Tag.parse(["stream", stream_name]),
                        Tag.parse(["seq", str(seq_num)]),
                        Tag.parse(["timestamp", str(observation.timestamp)]),
                        Tag.parse(["satori", "observation"]),
                    ]

                    builder = EventBuilder(
                        Kind(KIND_DATASTREAM_DATA),
                        encrypted_value,
                    ).tags(tags)

                    output = await self._client.send_event_builder(builder)
                    event_ids.append(output.id.to_hex())
                    self._stats["observations_sent"] += 1

                    if free:
                        sub_state.last_paid_seq = seq_num

                except Exception as e:
                    print(f"Error sending to {sub_pubkey}: {e}")

            # Zero-subscriber freshness fallback. When a paid stream has no
            # eligible recipients (all lapsed, none funded yet) the per-sub
            # loop emits nothing and the relay's last event ages out — the
            # marketplace then shows the stream as stale and no new
            # subscriber discovers it. Send one tags-only event so the relay
            # has a fresh created_at for freshness checks. No `p` tag, empty
            # body — leaks no value to non-paying subs.
            if not event_ids and stream_metadata.price_per_obs > 0:
                try:
                    tags = [
                        Tag.parse(["d", stream_name]),
                        Tag.parse(["stream", stream_name]),
                        Tag.parse(["seq", str(seq_num)]),
                        Tag.parse(["timestamp", str(observation.timestamp)]),
                        Tag.parse(["satori", "heartbeat"]),
                    ]
                    builder = EventBuilder(
                        Kind(KIND_DATASTREAM_DATA), ""
                    ).tags(tags)
                    output = await self._client.send_event_builder(builder)
                    event_ids.append(output.id.to_hex())
                except Exception as e:
                    print(f"Error sending heartbeat for {stream_name}: {e}")

        return event_ids

    def get_subscribers(self, stream_name: str) -> list[str]:
        """Get list of subscriber pubkeys for a stream (provider).

        Args:
            stream_name: Stream identifier

        Returns:
            List of subscriber public keys (hex)
        """
        stream_subscribers = self._subscribers.get(stream_name, {})
        return list(stream_subscribers.keys())

    def record_subscription(
        self,
        stream_name: str,
        subscriber_pubkey: str,
        payment_channel: str | None = None
    ) -> None:
        """Record a new subscriber (provider).

        Called when a subscription announcement is received.

        Args:
            stream_name: Stream identifier
            subscriber_pubkey: Subscriber's public key (hex)
            payment_channel: Optional payment channel info
        """
        if stream_name not in self._subscribers:
            self._subscribers[stream_name] = {}

        existing = self._subscribers[stream_name].get(subscriber_pubkey)
        self._subscribers[stream_name][subscriber_pubkey] = SubscriberState(
            subscriber_pubkey=subscriber_pubkey,
            stream_name=stream_name,
            # Preserve last_paid_seq so re-subscriptions (e.g. on reconnect)
            # don't revoke access the subscriber has already paid for.
            last_paid_seq=existing.last_paid_seq if existing else None,
            payment_channel=payment_channel,
            subscribed_at=existing.subscribed_at if existing else int(time.time()),
        )

    def record_payment(
        self,
        stream_name: str,
        subscriber_pubkey: str,
        seq_num: int
    ) -> None:
        """Record a payment from a subscriber (provider).

        Updates the subscriber's last_paid_seq to grant access to observations.

        Args:
            stream_name: Stream identifier
            subscriber_pubkey: Subscriber's public key (hex)
            seq_num: Observation sequence number they paid for
        """
        if stream_name in self._subscribers:
            if subscriber_pubkey in self._subscribers[stream_name]:
                sub_state = self._subscribers[stream_name][subscriber_pubkey]
                # Update to highest paid seq
                if sub_state.last_paid_seq is None or seq_num > sub_state.last_paid_seq:
                    sub_state.last_paid_seq = seq_num

    # ========================================================================
    # SUBSCRIBER APIs
    # ========================================================================

    async def discover_datastreams(
        self,
        tags: list[str] | None = None,
        limit: int = 100
    ) -> list[DatastreamMetadata]:
        """Discover available datastreams (subscriber).

        Queries relays for datastream announcements (kind 34600).

        Args:
            tags: Optional list of tags to filter by
            limit: Maximum number of results

        Returns:
            List of datastream metadata

        Raises:
            RuntimeError: If client not running
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")

        # Build filter
        filter_builder = Filter().kind(Kind(KIND_DATASTREAM_ANNOUNCE)).limit(limit)

        # Add tag filters if specified
        if tags:
            for tag in tags:
                filter_builder = filter_builder.hashtag(tag)

        # Query relays
        events_obj = await self._client.fetch_events(
            filter_builder, timedelta(seconds=10))
        events = events_obj.to_vec()

        # Parse metadata from events, skipping tombstoned (deleted) streams
        datastreams = []
        for event in events:
            try:
                # Skip events with status=deleted tag (tombstones)
                tag_values = [t.as_vec() for t in event.tags().to_vec()]
                if ["status", "deleted"] in tag_values:
                    continue
                metadata = DatastreamMetadata.from_json(event.content())
                datastreams.append(metadata)
            except Exception as e:
                print(f"Error parsing datastream metadata: {e}")

        return datastreams

    async def get_last_observation(self, stream_name: str) -> InboundObservation | None:
        """Get the latest observation for a stream from the relay.

        Queries relays for the most recent observation event (kind 34601)
        using the d-tag (stream_name). Returns the full parsed observation
        so callers can save it and process it like any other received observation.

        Args:
            stream_name: Stream identifier

        Returns:
            InboundObservation or None if no observations found

        Raises:
            RuntimeError: If client not running
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")

        # Query for latest observation by d-tag (stream_name)
        filter_builder = (
            Filter()
            .kind(Kind(KIND_DATASTREAM_DATA))
            .identifier(stream_name)
            .limit(1)
        )

        events_obj = await self._client.fetch_events(
            filter_builder, timedelta(seconds=10))
        events = events_obj.to_vec()

        if not events:
            return None

        event = events[0]
        try:
            sender_pubkey = event.author()
            content = event.content()
            # Empty content = heartbeat; no observation data to parse
            if not content:
                return None
            try:
                observation = DatastreamObservation.from_json(content)
            except Exception:
                obs_json = decrypt_observation(
                    content, sender_pubkey, self._keys)
                observation = DatastreamObservation.from_json(obs_json)
            return InboundObservation(
                stream_name=observation.stream_name,
                nostr_pubkey=sender_pubkey.to_hex(),
                observation=observation,
                event_id=event.id().to_hex(),
            )
        except Exception:
            return None

    async def get_last_observation_time(self, stream_name: str) -> int | None:
        """Get timestamp of the last published observation for a stream.

        Convenience wrapper around get_last_observation() for freshness checks.
        """
        obs = await self.get_last_observation(stream_name)
        if obs and obs.observation:
            return obs.observation.timestamp
        return None

    async def get_last_observation_event_time(
        self, stream_name: str
    ) -> int | None:
        """Get the nostr event timestamp of the last observation event.

        Reads only the public event header (created_at) — no decryption is
        attempted. This means freshness can be inferred for paid streams as
        well as free ones, as long as at least one observation event exists
        on the relay (i.e. the publisher has at least one paying subscriber
        for paid streams).

        Note: this is the relay event timestamp, not the publisher's
        observation.timestamp. They are typically within seconds of each
        other but can differ if the publisher backdates observations.

        Args:
            stream_name: Stream identifier

        Returns:
            Unix epoch seconds of the most recent observation event, or
            None if no observation events exist on the relay.
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")
        filter_builder = (
            Filter()
            .kind(Kind(KIND_DATASTREAM_DATA))
            .identifier(stream_name)
            .limit(1)
        )
        events_obj = await self._client.fetch_events(
            filter_builder, timedelta(seconds=10))
        events = events_obj.to_vec()
        if not events:
            return None
        try:
            return events[0].created_at().as_secs()
        except Exception:
            return None

    async def discover_active_datastreams(
        self,
        tags: list[str] | None = None,
        limit: int = 100,
        max_staleness_multiplier: float = 2.0
    ) -> list[DatastreamMetadata]:
        """Discover datastreams that are likely still active (subscriber).

        Queries relays for latest observation timestamps and filters based on cadence.

        Args:
            tags: Optional list of tags to filter by
            limit: Maximum number of results
            max_staleness_multiplier: How many cadence periods before considering stale

        Returns:
            List of datastream metadata that appear to be actively publishing

        Raises:
            RuntimeError: If client not running

        Example:
            >>> # Find active Bitcoin streams
            >>> active = await client.discover_active_datastreams(tags=["bitcoin"])
            >>> for stream in active:
            ...     last_time = await client.get_last_observation_time(stream.stream_name)
            ...     print(f"{stream.name}: last observation at {last_time}")
        """
        # Get all matching streams
        all_streams = await self.discover_datastreams(tags=tags, limit=limit)

        # Filter to only active streams by checking last observation time
        active_streams = []
        for stream in all_streams:
            last_obs_time = await self.get_last_observation_time(stream.stream_name)
            if last_obs_time and stream.is_likely_active(last_obs_time, max_staleness_multiplier):
                active_streams.append(stream)

        return active_streams

    async def subscribe_datastream(
        self,
        stream_name: str,
        provider_pubkey: str,
        payment_channel: str | None = None
    ) -> str:
        """Subscribe to a datastream (subscriber).

        Publishes a subscription announcement (kind 34602, public).

        Args:
            stream_name: Stream identifier to subscribe to
            provider_pubkey: Provider's public key (hex)
            payment_channel: Optional payment channel info

        Returns:
            Event ID of the subscription announcement

        Raises:
            RuntimeError: If client not running
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")

        # Create subscription announcement
        sub = SubscriptionAnnouncement(
            subscriber_pubkey=self.pubkey(),
            stream_name=stream_name,
            nostr_pubkey=provider_pubkey,
            timestamp=int(time.time()),
            payment_channel=payment_channel,
        )

        # Build tags
        tags = [
            Tag.parse(["d", stream_name]),  # Replaceable: latest sub state per stream
            Tag.parse(["p", provider_pubkey]),  # Tag provider
            Tag.parse(["stream", stream_name]),
            Tag.parse(["satori", "subscription"]),
        ]

        # Publish announcement
        builder = EventBuilder(
            Kind(KIND_SUBSCRIPTION_ANNOUNCE),
            sub.to_json(),
        ).tags(tags)

        output = await self._client.send_event_builder(builder)

        # Update statistics
        self._stats["subscriptions_announced"] += 1

        return output.id.to_hex()

    async def request_access(
        self,
        stream_name: str,
        producer_pubkey: str,
        message: str = '',
    ) -> str:
        """Request access to an approval-gated stream (subscriber).

        Sends an encrypted DM (kind 34609, NIP-04) directly to the producer.
        Not broadcast publicly — only the producer can decrypt it.

        Args:
            stream_name: Stream to request access to
            producer_pubkey: Producer's Nostr pubkey (hex)
            message: Optional message to the producer

        Returns:
            Event ID

        Raises:
            RuntimeError: If client not running
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")

        req = AccessRequest(
            stream_name=stream_name,
            requester_pubkey=self.pubkey(),
            producer_pubkey=producer_pubkey,
            message=message,
            timestamp=int(time.time()),
        )

        producer_pk = PublicKey.parse(producer_pubkey)
        from .encryption import encrypt_json
        encrypted = encrypt_json(req.to_json(), producer_pk, self._keys)

        tags = [
            Tag.parse(["p", producer_pubkey]),
            Tag.parse(["s", stream_name]),
            Tag.parse(["satori", "access_request"]),
        ]

        builder = EventBuilder(Kind(KIND_ACCESS_REQUEST), encrypted).tags(tags)
        output = await self._client.send_event_builder(builder)
        return output.id.to_hex()

    async def incoming_access_requests(self) -> AsyncIterator['InboundAccessRequest']:
        """Receive incoming access requests (producer).

        Yields InboundAccessRequest as subscribers request access.
        """
        while True:
            try:
                req = await asyncio.wait_for(
                    self._access_request_queue.get(), timeout=1.0)
                yield req
            except asyncio.TimeoutError:
                continue

    async def send_payment(
        self,
        provider_pubkey: str,
        stream_name: str,
        seq_num: int,
        amount_sats: int,
        tx_id: str | None = None
    ) -> str:
        """Send payment notification to provider (subscriber).

        Sends encrypted DM (kind 34603) to provider.

        Args:
            provider_pubkey: Provider's public key (hex)
            stream_name: Stream identifier
            seq_num: Observation sequence number being paid for
            amount_sats: Payment amount in satoshis
            tx_id: Optional transaction/payment proof

        Returns:
            Event ID of the payment notification

        Raises:
            RuntimeError: If client not running
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")

        # Create payment notification
        payment = PaymentNotification(
            from_pubkey=self.pubkey(),
            to_pubkey=provider_pubkey,
            stream_name=stream_name,
            seq_num=seq_num,
            amount_sats=amount_sats,
            timestamp=int(time.time()),
            tx_id=tx_id,
        )

        # Encrypt for provider
        payment_json = payment.to_json()
        recipient_pubkey = PublicKey.parse(provider_pubkey)
        encrypted = encrypt_payment(payment_json, recipient_pubkey, self._keys)

        # Build tags
        tags = [
            Tag.parse(["d", stream_name]),  # Replaceable: latest payment per stream
            Tag.parse(["p", provider_pubkey]),
            Tag.parse(["stream", stream_name]),
            Tag.parse(["seq", str(seq_num)]),
        ]

        # Send encrypted payment notification
        builder = EventBuilder(
            Kind(KIND_PAYMENT),
            encrypted,
        ).tags(tags)

        output = await self._client.send_event_builder(builder)

        # Update statistics
        self._stats["payments_sent"] += 1

        return output.id.to_hex()

    # ========================================================================
    # CHANNEL COMMITMENT APIs
    # ========================================================================

    async def publish_commitment(
        self,
        commitment: ChannelCommitment,
        receiver_nostr_pubkey: str,
    ) -> str:
        """Publish a partial transaction for the receiver to complete (buyer/sender).

        Publishes a kind 34604 parameterized replaceable event keyed by p2sh_address,
        so the relay keeps only the latest commitment per channel. Tagged with the
        receiver's Nostr pubkey so Nostr pushes it to them immediately.

        Note: `commitment.receiver_pubkey` is the 33-byte EVR wallet pubkey used
        to build the partial transaction; the Nostr `p` tag must be a 32-byte
        x-only Nostr pubkey, which is a separate value and must be passed in.

        Replaces Mantra's POST /channel/commitment.

        Args:
            commitment: The channel commitment containing the partial tx and sender sigs
            receiver_nostr_pubkey: Receiver's Nostr pubkey (hex) for push routing

        Returns:
            Event ID of the published commitment

        Raises:
            RuntimeError: If client not running
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")

        tags = [
            Tag.parse(["d", commitment.p2sh_address]),       # Replaceable: latest per channel
            Tag.parse(["p", receiver_nostr_pubkey]),         # Push to receiver (Nostr pubkey)
            Tag.parse(["satori", "commitment"]),
        ]
        if commitment.stream_name:
            tags.append(Tag.parse(["stream", commitment.stream_name]))

        builder = EventBuilder(
            Kind(KIND_CHANNEL_COMMITMENT),
            commitment.to_json(),
        ).tags(tags)

        output = await self._client.send_event_builder(builder)
        self._stats["commitments_sent"] += 1
        return output.id.to_hex()

    async def get_commitment(self, p2sh_address: str) -> ChannelCommitment | None:
        """Fetch the latest commitment for a specific channel.

        Replaces Mantra's GET /channel/commitment/:p2shAddress.

        Args:
            p2sh_address: The P2SH address identifying the channel

        Returns:
            The latest ChannelCommitment, or None if not found

        Raises:
            RuntimeError: If client not running
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")

        filter_obj = (
            Filter()
            .kind(Kind(KIND_CHANNEL_COMMITMENT))
            .identifier(p2sh_address)
            .limit(1)
        )

        events_obj = await self._client.fetch_events(filter_obj, timedelta(seconds=10))
        events = events_obj.to_vec()

        if not events:
            return None

        try:
            return ChannelCommitment.from_json(events[0].content())
        except Exception as e:
            print(f"Error parsing channel commitment: {e}")
            return None

    async def remove_commitment(self, p2sh_address: str) -> str:
        """Signal that a channel commitment has been claimed and broadcast.

        Publishes a tombstone event (same kind, same d-tag, empty content with
        'claimed' tag) that replaces the pending commitment on the relay.

        Replaces Mantra's DELETE /channel/commitment/:p2shAddress.

        Args:
            p2sh_address: The P2SH address identifying the channel

        Returns:
            Event ID of the tombstone event

        Raises:
            RuntimeError: If client not running
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")

        tags = [
            Tag.parse(["d", p2sh_address]),
            Tag.parse(["action", "claimed"]),
            Tag.parse(["satori", "commitment"]),
        ]

        builder = EventBuilder(
            Kind(KIND_CHANNEL_COMMITMENT),
            "",
        ).tags(tags)

        output = await self._client.send_event_builder(builder)
        return output.id.to_hex()

    async def publish_channel_open(
        self,
        channel_open: ChannelOpen,
        receiver_nostr_pubkey: str,
    ) -> str:
        """Announce a new channel to the receiver via Nostr (sender side).

        Publishes a kind 34605 parameterized replaceable event so the receiver
        can save the channel to their DB and process future commitments.

        Args:
            channel_open: Channel metadata (EVR wallet pubkeys, redeem script, etc.)
            receiver_nostr_pubkey: Receiver's Nostr pubkey (hex) for push routing

        Returns:
            Event ID of the announcement
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")

        tags = [
            Tag.parse(["d", channel_open.p2sh_address]),
            Tag.parse(["p", receiver_nostr_pubkey]),
            Tag.parse(["satori", "channel_open"]),
        ]

        builder = EventBuilder(
            Kind(KIND_CHANNEL_OPEN),
            channel_open.to_json(),
        ).tags(tags)

        output = await self._client.send_event_builder(builder)
        return output.id.to_hex()

    async def channel_opens(self) -> AsyncIterator[InboundChannelOpen]:
        """Receive incoming channel open announcements (receiver side).

        Yields channel open announcements pushed by senders in real time.

        Yields:
            InboundChannelOpen instances as they arrive

        Raises:
            RuntimeError: If client not running
        """
        if not self._running:
            raise RuntimeError("Client not running")

        while self._running:
            try:
                item = await asyncio.wait_for(
                    self._channel_open_queue.get(), timeout=1.0)
                yield item
            except asyncio.TimeoutError:
                continue

    async def publish_settlement(
        self,
        settlement: ChannelSettlement,
        sender_nostr_pubkey: str,
    ) -> str:
        """Notify the sender that a channel was claimed and provide new UTXO info."""
        if not self._running or not self._client:
            raise RuntimeError("Client not running")
        tags = [
            Tag.parse(["d", settlement.p2sh_address]),
            Tag.parse(["p", sender_nostr_pubkey]),
            Tag.parse(["satori", "settlement"]),
        ]
        builder = EventBuilder(
            Kind(KIND_CHANNEL_SETTLED),
            settlement.to_json(),
        ).tags(tags)
        output = await self._client.send_event_builder(builder)
        return output.id.to_hex()

    async def settlements(self) -> AsyncIterator[InboundChannelSettlement]:
        """Receive incoming channel settlement notifications (sender side)."""
        if not self._running:
            raise RuntimeError("Client not running")
        while self._running:
            try:
                item = await asyncio.wait_for(
                    self._settlement_queue.get(), timeout=1.0)
                yield item
            except asyncio.TimeoutError:
                continue

    async def commitments(self) -> AsyncIterator[InboundCommitment]:
        """Receive incoming channel commitments as they arrive (seller/receiver).

        Yields commitments pushed by buyers in real time. No polling required —
        Nostr delivers events tagged with this client's pubkey immediately.

        Replaces Mantra's GET /channel/commitments/:receiverPubkey (polling).

        Yields:
            InboundCommitment instances as they arrive

        Raises:
            RuntimeError: If client not running
        """
        if not self._running:
            raise RuntimeError("Client not running")

        while self._running:
            try:
                commitment = await asyncio.wait_for(
                    self._commitment_queue.get(), timeout=1.0)
                yield commitment
            except asyncio.TimeoutError:
                continue

    # ========================================================================
    # CONSUMER APIs (async iteration)
    # ========================================================================

    async def observations(self) -> AsyncIterator[InboundObservation]:
        """Receive observations from subscribed datastreams (subscriber).

        Yields:
            InboundObservation instances as they arrive

        Raises:
            RuntimeError: If client not running
        """
        if not self._running:
            raise RuntimeError("Client not running")

        while self._running:
            try:
                obs = await asyncio.wait_for(self._observation_queue.get(), timeout=1.0)
                yield obs
            except asyncio.TimeoutError:
                continue

    async def payments(self) -> AsyncIterator[InboundPayment]:
        """Receive payment notifications (provider).

        Yields:
            InboundPayment instances as they arrive

        Raises:
            RuntimeError: If client not running
        """
        if not self._running:
            raise RuntimeError("Client not running")

        while self._running:
            try:
                payment = await asyncio.wait_for(self._payment_queue.get(), timeout=1.0)
                yield payment
            except asyncio.TimeoutError:
                continue

    # ========================================================================
    # UTILITY APIs
    # ========================================================================

    async def get_datastream(self, stream_name: str) -> DatastreamMetadata | None:
        """Get metadata for a specific datastream by ID.

        Args:
            stream_name: Stream identifier to look up

        Returns:
            DatastreamMetadata if found, None otherwise

        Raises:
            RuntimeError: If client not running
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")

        # Query for this specific stream (d-tag is the replaceable event identifier)
        filter_obj = Filter().kind(Kind(KIND_DATASTREAM_ANNOUNCE)).identifier(stream_name)

        events_obj = await self._client.fetch_events(
            filter_obj, timedelta(seconds=10))
        events = events_obj.to_vec()

        if events:
            try:
                # Return the most recent announcement
                latest_event = max(events, key=lambda e: e.created_at())
                return DatastreamMetadata.from_json(latest_event.content())
            except Exception as e:
                print(f"Error parsing datastream metadata: {e}")

        return None

    def list_announced_streams(self) -> list[DatastreamMetadata]:
        """List all datastreams announced by this client.

        Returns:
            List of announced datastream metadata
        """
        return list(self._announced_streams.values())

    async def unsubscribe_datastream(self, stream_name: str, provider_pubkey: str) -> str:
        """Unsubscribe from a datastream.

        Publishes an unsubscription announcement (kind 34602 with unsubscribe tag).

        Args:
            stream_name: Stream identifier to unsubscribe from
            provider_pubkey: Provider's public key (hex)

        Returns:
            Event ID of the unsubscription announcement

        Raises:
            RuntimeError: If client not running
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")

        # Create unsubscription announcement
        unsub = SubscriptionAnnouncement(
            subscriber_pubkey=self.pubkey(),
            stream_name=stream_name,
            nostr_pubkey=provider_pubkey,
            timestamp=int(time.time()),
            payment_channel=None,
        )

        # Build tags with unsubscribe marker
        tags = [
            Tag.parse(["d", stream_name]),  # Replaces the subscription announcement
            Tag.parse(["p", provider_pubkey]),
            Tag.parse(["stream", stream_name]),
            Tag.parse(["action", "unsubscribe"]),  # Mark as unsubscribe
            Tag.parse(["satori", "unsubscription"]),
        ]

        # Publish announcement
        builder = EventBuilder(
            Kind(KIND_SUBSCRIPTION_ANNOUNCE),
            unsub.to_json(),
        ).tags(tags)

        output = await self._client.send_event_builder(builder)

        return output.id.to_hex()

    def get_statistics(self) -> dict[str, int]:
        """Get client statistics.

        Returns:
            Dictionary with statistics:
            - observations_sent: Number of observations published
            - observations_received: Number of observations received
            - payments_sent: Number of payments sent
            - payments_received: Number of payments received
            - subscriptions_announced: Number of subscriptions announced
        """
        return self._stats.copy()

    def get_subscriber_info(self, stream_name: str, subscriber_pubkey: str) -> SubscriberState | None:
        """Get information about a specific subscriber (provider).

        Args:
            stream_name: Stream identifier
            subscriber_pubkey: Subscriber's public key (hex)

        Returns:
            SubscriberState if subscriber exists, None otherwise
        """
        stream_subs = self._subscribers.get(stream_name, {})
        return stream_subs.get(subscriber_pubkey)

    def get_all_subscribers_info(self, stream_name: str) -> dict[str, SubscriberState]:
        """Get information about all subscribers to a stream (provider).

        Args:
            stream_name: Stream identifier

        Returns:
            Dictionary mapping subscriber pubkeys to their SubscriberState
        """
        return self._subscribers.get(stream_name, {}).copy()

    # ========================================================================
    # INTERNAL - Event Processing
    # ========================================================================

    async def _event_listener(self) -> None:
        """Background task that listens for incoming Nostr events."""
        if not self._client:
            return

        # Subscribe to all Satori event kinds
        satori_filter = Filter().kinds([
            Kind(KIND_DATASTREAM_DATA),
            Kind(KIND_SUBSCRIPTION_ANNOUNCE),
            Kind(KIND_PAYMENT),
            Kind(KIND_CHANNEL_COMMITMENT),
            Kind(KIND_CHANNEL_OPEN),
            Kind(KIND_CHANNEL_SETTLED),
            Kind(KIND_PREDICTION),
            Kind(KIND_ACCESS_REQUEST),
        ])
        await self._client.subscribe(satori_filter)

        # Process events via callback handler
        client_ref = self

        class _Handler(HandleNotification):
            async def handle(self, relay_url, subscription_id, event):
                await client_ref._handle_event(event)

            async def handle_msg(self, relay_url, msg):
                pass

        try:
            await self._client.handle_notifications(_Handler())
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"Error in event listener: {e}")

    async def _handle_event(self, event: Event) -> None:
        """Handle a received Nostr event.

        Args:
            event: Nostr event to process
        """
        # Check for duplicates
        event_id = event.id().to_hex()
        if self._dedupe.is_seen(event_id):
            return
        self._dedupe.mark_seen(event_id)

        kind = event.kind().as_u16()

        if kind == KIND_DATASTREAM_DATA:
            # Observation data (encrypted)
            await self._handle_observation_event(event)
        elif kind == KIND_PAYMENT:
            # Payment notification (encrypted)
            await self._handle_payment_event(event)
        elif kind == KIND_SUBSCRIPTION_ANNOUNCE:
            # Subscription announcement (public)
            await self._handle_subscription_event(event)
        elif kind == KIND_CHANNEL_COMMITMENT:
            # Channel commitment — partial tx from buyer to seller
            await self._handle_commitment_event(event)
        elif kind == KIND_CHANNEL_OPEN:
            # Channel open announcement — sender informs receiver of new channel
            await self._handle_channel_open_event(event)
        elif kind == KIND_CHANNEL_SETTLED:
            await self._handle_settlement_event(event)
        elif kind == KIND_PREDICTION:
            await self._handle_prediction_event(event)
        elif kind == KIND_ACCESS_REQUEST:
            await self._handle_access_request_event(event)

    async def _handle_observation_event(self, event: Event) -> None:
        """Handle an observation data event (kind 34601).

        Free streams: content is plaintext JSON (full DatastreamObservation).
        Paid streams: metadata in plaintext tags, only value encrypted in content.
        """
        try:
            sender_pubkey = event.author()
            content = event.content()

            # Empty content = legacy heartbeat, skip
            if not content:
                return

            tag_map = {}
            for tag in event.tags().to_vec():
                vec = tag.as_vec()
                if len(vec) >= 2:
                    tag_map[vec[0]] = vec[1]

            # Try plaintext JSON first (free stream)
            try:
                observation = DatastreamObservation.from_json(content)
            except Exception:
                # Paid stream: encrypted for a specific recipient — skip if
                # the p tag doesn't match our own pubkey (e.g. we're the
                # publisher seeing our own event re-delivered by the relay)
                p_tag = tag_map.get("p", "")
                if p_tag and p_tag != self._keys.public_key().to_hex():
                    return
                # Decrypt value, reconstruct from tags
                stream_name = tag_map.get("stream") or tag_map.get("d", "")
                seq_num = int(tag_map.get("seq", 0))
                timestamp = int(tag_map.get("timestamp", 0))
                if not stream_name:
                    return
                value_str = decrypt_observation(content, sender_pubkey, self._keys)
                observation = DatastreamObservation(
                    stream_name=stream_name,
                    timestamp=timestamp,
                    value=value_str,
                    seq_num=seq_num,
                )

            inbound = InboundObservation(
                stream_name=observation.stream_name,
                nostr_pubkey=sender_pubkey.to_hex(),
                observation=observation,
                event_id=event.id().to_hex(),
            )

            await self._observation_queue.put(inbound)
            self._stats["observations_received"] += 1

        except EncryptionError as e:
            print(f"Failed to decrypt observation: {e}")
        except Exception as e:
            print(f"Error handling observation event: {e}")

    async def _handle_payment_event(self, event: Event) -> None:
        """Handle a payment notification event (kind 34603)."""
        try:
            # Decrypt payment
            sender_pubkey = event.author()
            encrypted_content = event.content()

            payment_json = decrypt_payment(encrypted_content, sender_pubkey, self._keys)
            payment = PaymentNotification.from_json(payment_json)

            # Record payment in subscriber state
            self.record_payment(
                payment.stream_name,
                payment.from_pubkey,
                payment.seq_num
            )

            # Create inbound payment
            inbound = InboundPayment(
                payment=payment,
                event_id=event.id().to_hex(),
            )

            # Queue for consumer
            await self._payment_queue.put(inbound)

            # Update statistics
            self._stats["payments_received"] += 1

        except EncryptionError as e:
            print(f"Failed to decrypt payment: {e}")
        except Exception as e:
            print(f"Error handling payment event: {e}")

    async def _handle_prediction_event(self, event: Event) -> None:
        """Handle an incoming prediction submission (kind 34608, host side).

        Only the host can decrypt — others silently discard.
        """
        try:
            sender_pubkey = event.author()
            from .encryption import decrypt_json, EncryptionError as _EncErr
            prediction_json = decrypt_json(
                event.content(), sender_pubkey, self._keys)
            prediction = PredictionSubmission.from_json(prediction_json)
            inbound = InboundPrediction(
                prediction=prediction,
                event_id=event.id().to_hex(),
            )
            await self._prediction_queue.put(inbound)
        except Exception:
            pass  # not the intended recipient — silently discard

    async def _handle_subscription_event(self, event: Event) -> None:
        """Handle a subscription announcement event (kind 34602).

        Subscription events with an action=unsubscribe tag remove the
        subscriber from the in-memory _subscribers dict (Fix G).
        """
        try:
            # Check for unsubscribe action tag
            action = None
            for tag in event.tags().to_vec():
                vec = tag.as_vec()
                if len(vec) >= 2 and vec[0] == 'action':
                    action = vec[1]
                    break

            # Parse subscription (public content)
            sub = SubscriptionAnnouncement.from_json(event.content())

            # If I'm the provider, handle subscribe or unsubscribe
            if sub.nostr_pubkey == self.pubkey():
                if action == 'unsubscribe':
                    if (sub.stream_name in self._subscribers
                            and sub.subscriber_pubkey
                            in self._subscribers[sub.stream_name]):
                        del self._subscribers[sub.stream_name][
                            sub.subscriber_pubkey]
                else:
                    self.record_subscription(
                        sub.stream_name,
                        sub.subscriber_pubkey,
                        sub.payment_channel
                    )

        except Exception as e:
            print(f"Error handling subscription event: {e}")

    async def tombstones(self) -> AsyncIterator[str]:
        """Receive p2sh_address strings for tombstoned commitments (sender side).

        Yields the p2sh_address of each channel whose commitment has been
        replaced by a tombstone on the relay, indicating the receiver claimed.
        Used as a fallback reset when KIND_CHANNEL_SETTLED is not received.

        Yields:
            p2sh_address strings as they arrive
        """
        if not self._running:
            raise RuntimeError("Client not running")
        while self._running:
            try:
                p2sh = await asyncio.wait_for(
                    self._tombstone_queue.get(), timeout=1.0)
                yield p2sh
            except asyncio.TimeoutError:
                continue

    async def _handle_commitment_event(self, event: Event) -> None:
        """Handle a channel commitment event (kind 34604).

        Tombstone events (empty content) are queued to _tombstone_queue so
        the sender can use them as a fallback reset if KIND_CHANNEL_SETTLED
        was not received. All other commitments are queued unconditionally —
        the consumer (_channelProcessCommitment in start.py) filters them by
        looking up the channel row by p2sh_address, which inherently proves
        ownership.

        The payload's `receiver_pubkey` is a 33-byte EVR wallet pubkey and
        cannot be compared to `self.pubkey()` (a 32-byte Nostr pubkey), so the
        payload itself cannot be used to filter at this layer. The event *tag*
        `p`, however, is set by publish_commitment() to the receiver's Nostr
        pubkey and could be used for a library-side filter — see the
        `commitment p-tag filter` item in tasks/paid-stream-fixes.md.
        """
        try:
            content = event.content()

            # Tombstone — queue the p2sh_address as a fallback reset signal
            if not content:
                try:
                    for tag in event.tags().to_vec():
                        tag_vec = tag.as_vec()
                        if len(tag_vec) >= 2 and tag_vec[0] == 'd':
                            await self._tombstone_queue.put(tag_vec[1])
                            break
                except Exception:
                    pass
                return

            # Fix O: drop commitments not addressed to us. The `p` tag is
            # set by publish_commitment() to the receiver's Nostr pubkey.
            # Without this filter, stale third-party commitments on the relay
            # trigger unnecessary DB lookups and 3-second retries on reconnect.
            p_tag = None
            for tag in event.tags().to_vec():
                tag_vec = tag.as_vec()
                if len(tag_vec) >= 2 and tag_vec[0] == 'p':
                    p_tag = tag_vec[1]
                    break
            if p_tag and p_tag != self.pubkey():
                return

            commitment = ChannelCommitment.from_json(content)

            inbound = InboundCommitment(
                commitment=commitment,
                event_id=event.id().to_hex(),
            )

            await self._commitment_queue.put(inbound)
            self._stats["commitments_received"] += 1

        except Exception as e:
            print(f"Error handling commitment event: {e}")

    async def _handle_channel_open_event(self, event: Event) -> None:
        """Handle a channel open announcement event (kind 34605).

        Queues all valid open announcements. The consumer (_channelHandleOpen
        in start.py) filters to those addressed to this node's EVR wallet pubkey.
        """
        try:
            content = event.content()
            if not content:
                return
            channel_open = ChannelOpen.from_json(content)
            inbound = InboundChannelOpen(
                channel_open=channel_open,
                event_id=event.id().to_hex(),
            )
            await self._channel_open_queue.put(inbound)
        except Exception as e:
            print(f"Error handling channel open event: {e}")

    async def _handle_settlement_event(self, event: Event) -> None:
        """Handle a channel settlement notification event (kind 34606)."""
        try:
            content = event.content()
            if not content:
                return
            settlement = ChannelSettlement.from_json(content)
            inbound = InboundChannelSettlement(
                settlement=settlement,
                event_id=event.id().to_hex(),
            )
            await self._settlement_queue.put(inbound)
        except Exception as e:
            print(f"Error handling settlement event: {e}")

    async def _handle_access_request_event(self, event: Event) -> None:
        """Handle an incoming access request (kind 34609, producer side).

        Only the producer can decrypt — others silently discard.
        """
        try:
            sender_pubkey = event.author()
            from .encryption import decrypt_json
            request_json = decrypt_json(
                event.content(), sender_pubkey, self._keys)
            access_request = AccessRequest.from_json(request_json)
            inbound = InboundAccessRequest(
                access_request=access_request,
                event_id=event.id().to_hex(),
            )
            await self._access_request_queue.put(inbound)
        except Exception:
            pass  # not the intended recipient — silently discard
