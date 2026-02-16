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
)

from .models import (
    SatoriNostrConfig,
    DatastreamMetadata,
    DatastreamObservation,
    SubscriptionAnnouncement,
    PaymentNotification,
    InboundObservation,
    InboundPayment,
    KIND_DATASTREAM_ANNOUNCE,
    KIND_DATASTREAM_DATA,
    KIND_SUBSCRIPTION_ANNOUNCE,
    KIND_PAYMENT,
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

        # Statistics
        self._stats = {
            "observations_sent": 0,
            "observations_received": 0,
            "payments_sent": 0,
            "payments_received": 0,
            "subscriptions_announced": 0,
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
        self._client = Client(self._keys)

        # Add relays
        for relay_url in self.config.relay_urls:
            await self._client.add_relay(relay_url)

        # Connect to relays
        await self._client.connect()

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

    async def announce_datastream(self, metadata: DatastreamMetadata) -> str:
        """Announce a datastream (provider).

        Publishes stream metadata as a kind 30100 event (public, discoverable).

        Args:
            metadata: Datastream metadata to announce

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

        # Add topic tags for discovery
        for tag in metadata.tags:
            tags.append(Tag.parse(["t", tag]))

        # Add stream topic tag
        tags.append(Tag.parse(["stream", compute_stream_topic_tag(metadata.stream_name)]))

        # Build event with metadata as content
        event = EventBuilder(
            Kind(KIND_DATASTREAM_ANNOUNCE),
            metadata.to_json(),
            tags
        ).to_event(self._keys)

        # Publish to relays
        event_id = await self._client.send_event(event)

        # Track announced stream
        self._announced_streams[metadata.stream_name] = metadata

        return event_id.to_hex()

    async def publish_observation(
        self,
        observation: DatastreamObservation,
        stream_metadata: DatastreamMetadata
    ) -> list[str]:
        """Publish an observation to all paid subscribers (provider).

        Sends encrypted DMs (kind 30101) to each subscriber who has paid
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
                Tag.parse(["stream", stream_name]),
                Tag.parse(["seq", str(seq_num)]),
                Tag.parse(["satori", "observation"]),
            ]

            event = EventBuilder(
                Kind(KIND_DATASTREAM_DATA),
                content,
                tags
            ).to_event(self._keys)

            event_id = await self._client.send_event(event)
            event_ids.append(event_id.to_hex())
            self._stats["observations_sent"] += 1
        else:
            # Paid stream: encrypt and send per subscriber
            for sub_pubkey, sub_state in stream_subscribers.items():
                if sub_state.last_paid_seq is not None and sub_state.last_paid_seq >= seq_num:
                    try:
                        recipient_pubkey = PublicKey.parse(sub_pubkey)
                        encrypted = encrypt_observation(
                            obs_json, recipient_pubkey, self._keys)

                        tags = [
                            Tag.parse(["p", sub_pubkey]),
                            Tag.parse(["stream", stream_name]),
                            Tag.parse(["seq", str(seq_num)]),
                        ]

                        event = EventBuilder(
                            Kind(KIND_DATASTREAM_DATA),
                            encrypted,
                            tags
                        ).to_event(self._keys)

                        event_id = await self._client.send_event(event)
                        event_ids.append(event_id.to_hex())
                        self._stats["observations_sent"] += 1

                    except Exception as e:
                        print(f"Error sending to {sub_pubkey}: {e}")

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

        self._subscribers[stream_name][subscriber_pubkey] = SubscriberState(
            subscriber_pubkey=subscriber_pubkey,
            stream_name=stream_name,
            last_paid_seq=None,
            payment_channel=payment_channel,
            subscribed_at=int(time.time()),
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

        Queries relays for datastream announcements (kind 30100).

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
        events = await self._client.get_events_of([filter_builder])

        # Parse metadata from events
        datastreams = []
        for event in events:
            try:
                metadata = DatastreamMetadata.from_json(event.content())
                datastreams.append(metadata)
            except Exception as e:
                print(f"Error parsing datastream metadata: {e}")

        return datastreams

    async def get_last_observation_time(self, stream_name: str) -> int | None:
        """Get timestamp of the last published observation for a stream.

        Queries relays for the most recent observation event (kind 30101) for the stream.
        Note: Event timestamps are public even if content is encrypted.

        Args:
            stream_name: Stream identifier

        Returns:
            Unix timestamp of last observation, or None if no observations found

        Raises:
            RuntimeError: If client not running

        Example:
            >>> last_time = await client.get_last_observation_time("btc-price")
            >>> if last_time:
            ...     age = time.time() - last_time
            ...     print(f"Last observation was {age} seconds ago")
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")

        # Query for latest observation event for this stream
        # The event timestamp is public even if content is encrypted
        filter_builder = (
            Filter()
            .kind(Kind(KIND_DATASTREAM_DATA))
            .custom_tag("stream", stream_name)
            .limit(1)
        )

        events = await self._client.get_events_of([filter_builder])

        if events:
            # Nostr SDK returns events sorted by timestamp (newest first)
            return events[0].created_at().as_secs()

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

        Publishes a subscription announcement (kind 30102, public).

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
            Tag.parse(["p", provider_pubkey]),  # Tag provider
            Tag.parse(["stream", stream_name]),
            Tag.parse(["satori", "subscription"]),
        ]

        # Publish announcement
        event = EventBuilder(
            Kind(KIND_SUBSCRIPTION_ANNOUNCE),
            sub.to_json(),
            tags
        ).to_event(self._keys)

        event_id = await self._client.send_event(event)

        # Update statistics
        self._stats["subscriptions_announced"] += 1

        return event_id.to_hex()

    async def send_payment(
        self,
        provider_pubkey: str,
        stream_name: str,
        seq_num: int,
        amount_sats: int,
        tx_id: str | None = None
    ) -> str:
        """Send payment notification to provider (subscriber).

        Sends encrypted DM (kind 30103) to provider.

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
            Tag.parse(["p", provider_pubkey]),
            Tag.parse(["stream", stream_name]),
            Tag.parse(["seq", str(seq_num)]),
        ]

        # Send encrypted payment notification
        event = EventBuilder(
            Kind(KIND_PAYMENT),
            encrypted,
            tags
        ).to_event(self._keys)

        event_id = await self._client.send_event(event)

        # Update statistics
        self._stats["payments_sent"] += 1

        return event_id.to_hex()

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

        # Query for this specific stream
        filter_obj = Filter().kind(Kind(KIND_DATASTREAM_ANNOUNCE)).custom_tag("d", [stream_name])

        events = await self._client.get_events_of([filter_obj])

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

        Publishes an unsubscription announcement (kind 30102 with unsubscribe tag).

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
            Tag.parse(["p", provider_pubkey]),
            Tag.parse(["stream", stream_name]),
            Tag.parse(["action", "unsubscribe"]),  # Mark as unsubscribe
            Tag.parse(["satori", "unsubscription"]),
        ]

        # Publish announcement
        event = EventBuilder(
            Kind(KIND_SUBSCRIPTION_ANNOUNCE),
            unsub.to_json(),
            tags
        ).to_event(self._keys)

        event_id = await self._client.send_event(event)

        return event_id.to_hex()

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

        # Subscribe to relevant event kinds
        filters = [
            # Encrypted observations sent to me (kind 30101, tagged with my pubkey)
            Filter().kind(Kind(KIND_DATASTREAM_DATA)).pubkey(self._keys.public_key()),
            # Free broadcast observations (kind 30101, no p-tag / not addressed to anyone specific)
            Filter().kind(Kind(KIND_DATASTREAM_DATA)).custom_tag("satori", "observation"),
            # Payments sent to me (kind 30103)
            Filter().kind(Kind(KIND_PAYMENT)).pubkey(self._keys.public_key()),
            # Subscription announcements (kind 30102) - if I'm a provider
            Filter().kind(Kind(KIND_SUBSCRIPTION_ANNOUNCE)),
        ]

        await self._client.subscribe(filters)

        # Process events
        while self._running:
            try:
                # Get notifications from relays
                notifications = await self._client.notifications()

                async for notification in notifications:
                    if notification.is_event():
                        event = notification.as_event()
                        await self._handle_event(event)

            except Exception as e:
                print(f"Error in event listener: {e}")
                await asyncio.sleep(1)

    async def _handle_event(self, event: Event) -> None:
        """Handle a received Nostr event.

        Args:
            event: Nostr event to process
        """
        # Check for duplicates
        event_id = event.id().to_hex()
        if self._dedupe.contains(event_id):
            return
        self._dedupe.add(event_id)

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

    async def _handle_observation_event(self, event: Event) -> None:
        """Handle an observation data event (kind 30101).

        Free streams broadcast plaintext content.
        Paid streams encrypt per-subscriber via NIP-04.
        """
        try:
            sender_pubkey = event.author()
            content = event.content()

            # Try plaintext first (free broadcast), fall back to decryption (paid)
            try:
                observation = DatastreamObservation.from_json(content)
            except Exception:
                # Not valid JSON / not plaintext - try decrypting
                obs_json = decrypt_observation(content, sender_pubkey, self._keys)
                observation = DatastreamObservation.from_json(obs_json)

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
        """Handle a payment notification event (kind 30103)."""
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

    async def _handle_subscription_event(self, event: Event) -> None:
        """Handle a subscription announcement event (kind 30102)."""
        try:
            # Parse subscription (public content)
            sub = SubscriptionAnnouncement.from_json(event.content())

            # If I'm the provider, record this subscription
            if sub.nostr_pubkey == self.pubkey():
                self.record_subscription(
                    sub.stream_name,
                    sub.subscriber_pubkey,
                    sub.payment_channel
                )

        except Exception as e:
            print(f"Error handling subscription event: {e}")
