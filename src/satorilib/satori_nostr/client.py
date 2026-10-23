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
    RelayUrl,
    ReqTarget,
    SingleLetterTag,
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
    BountyAnnouncement,
    PredictionSubmission,
    InboundPrediction,
    InboundObservation,
    InboundPayment,
    InboundSubscription,
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
    KIND_BOUNTY_ANNOUNCE,
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

# Satori relays (embedded strfry) return at most maxFilterLimit = 500
# events per filter and accept at most maxReqFilterSize = 200 filters
# per REQ.
DISCOVERY_PAGE_SIZE = 500
FILTER_CHUNK_SIZE = 200
FOLLOW_SUBSCRIPTION_ID = "satori-follow"


def _d_tag(event: Event) -> str:
    """Return the event's `d` tag value, or '' if it has none."""
    for tag in event.tags():
        vec = tag.to_vec()
        if len(vec) >= 2 and vec[0] == "d":
            return vec[1]
    return ""


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

        # Track announced streams (for providers)
        self._announced_streams: dict[str, DatastreamMetadata] = {}

        # Event queues for async iteration
        self._observation_queue: asyncio.Queue[InboundObservation] = asyncio.Queue()
        self._payment_queue: asyncio.Queue[InboundPayment] = asyncio.Queue()
        self._subscription_queue: asyncio.Queue[InboundSubscription] = asyncio.Queue()
        self._commitment_queue: asyncio.Queue[InboundCommitment] = asyncio.Queue()
        self._channel_open_queue: asyncio.Queue[InboundChannelOpen] = asyncio.Queue()
        self._settlement_queue: asyncio.Queue[InboundChannelSettlement] = asyncio.Queue()
        self._tombstone_queue: asyncio.Queue[str] = asyncio.Queue()  # p2sh_address strings
        self._prediction_queue: asyncio.Queue[InboundPrediction] = asyncio.Queue()
        self._access_request_queue: asyncio.Queue[InboundAccessRequest] = asyncio.Queue()

        # Authors whose public events (observations, commitment tombstones)
        # this client follows; see set_followed_authors().
        self._followed_authors: list[str] | None = None

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

    async def _send_builder(self, builder: EventBuilder) -> Event:
        """Sign an EventBuilder with our keys and publish it.

        The one place signing happens. nostr-sdk 0.45 removed
        `Client.send_event_builder`, which used to do build-sign-send in one
        step using a signer held by the client. The client no longer holds one,
        so the three steps are explicit here rather than repeated at every call
        site.

        Returns the SIGNED EVENT rather than the send result: 0.45's
        `SendEventOutput` exposes no id, and the id is what every caller
        actually wanted.
        """
        unsigned = builder.finalize_unsigned(self._keys.public_key())
        signed = self._keys.sign_event(unsigned)
        await self._client.send_event(signed)
        return signed

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

        # nostr-sdk 0.45 took the signer off the client: `Client()` has no
        # arguments, `NostrSigner` became a protocol rather than a factory, and
        # `send_event_builder` is gone. Events are now built, signed with the
        # keys, and sent already-signed — see `_send_builder`.
        self._client = Client()

        # Add relays
        for relay_url in self.config.relay_urls:
            await self._client.add_relay(RelayUrl.parse(relay_url))

        # Connect to relays and wait for connection.
        #
        # 0.45 folded `wait_for_connection(timeout)` into `connect(and_wait)`:
        # passing a duration connects AND waits, so the two calls became one.
        await self._client.connect(timedelta(seconds=5))

        # Open the notification stream before subscribing so no event that
        # arrives in between is missed, then subscribe here (not in the
        # listener task) so set_followed_authors() can never be overtaken
        # by the default subscription.
        notifications = self._client.notifications()
        await self._subscribe_defaults()
        self._listener_task = asyncio.create_task(
            self._event_listener(notifications))

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
        output = await self._send_builder(builder)

        if not deleted:
            # Track announced stream
            self._announced_streams[metadata.stream_name] = metadata

        return output.id().to_hex()

    async def announce_bounty(self, bounty: BountyAnnouncement) -> str:
        """Announce a prediction bounty (host).

        Publishes bounty metadata as a kind 34607 parameterized replaceable
        event. Re-publishing with the same d-tag replaces the previous announcement.

        Args:
            bounty: Bounty announcement to publish

        Returns:
            Event ID of the announcement

        Raises:
            RuntimeError: If client not running
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")

        tags = [
            Tag.parse(["d", bounty.d_tag()]),
            Tag.parse(["satori", "bounty"]),
            Tag.parse(["s", bounty.stream_name]),
            Tag.parse(["p", bounty.stream_provider_pubkey]),
        ]

        builder = EventBuilder(
            Kind(KIND_BOUNTY_ANNOUNCE),
            bounty.to_json(),
        ).tags(tags)

        output = await self._send_builder(builder)
        return output.id().to_hex()

    async def close_bounty(self, bounty: BountyAnnouncement) -> str:
        """Close a bounty by publishing an inactive replacement (host).

        Args:
            bounty: The bounty to close (active flag will be set False)

        Returns:
            Event ID of the closing event

        Raises:
            RuntimeError: If client not running
        """
        return await self.announce_bounty(bounty.close())

    async def publish_relay_list(self, relay_urls: list[str]) -> str:
        """Publish a NIP-65 relay list metadata event (kind 10002).

        This is a replaceable event — each publication overwrites the
        previous one. Other clients and the central server can subscribe
        to kind 10002 events from this node's pubkey to discover which
        relays it uses.

        Args:
            relay_urls: List of relay WebSocket URLs to advertise.

        Returns:
            Event ID of the published relay list event.

        Raises:
            RuntimeError: If client not running.
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")

        tags = [Tag.parse(["r", url]) for url in relay_urls]

        builder = EventBuilder(Kind(10002), "").tags(tags)
        output = await self._send_builder(builder)
        return output.id().to_hex()

    async def discover_bounties(
        self,
        stream_name: str | None = None,
        active_only: bool = False,
        limit: int = 100,
    ) -> list[BountyAnnouncement]:
        """Discover available prediction bounties (predictor/observer).

        Queries relays for bounty announcements (kind 34607).

        Args:
            stream_name: Optional stream name to filter by
            active_only: If True, only return bounties with active=True
            limit: Maximum number of results

        Returns:
            List of BountyAnnouncement

        Raises:
            RuntimeError: If client not running
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")

        filter_builder = Filter().kind(Kind(KIND_BOUNTY_ANNOUNCE)).limit(limit)
        if stream_name:
            # nostr-sdk 0.45 dropped the `Alphabet` enum and
            # `SingleLetterTag.lowercase(...)`; a tag is now built from its byte.
            # `ord("s")` is lowercase 's' by construction, which is what the old
            # `Alphabet.S` + lowercase() pair spelled out.
            filter_builder = filter_builder.custom_tag(
                SingleLetterTag.from_byte(ord("s")), stream_name)

        events = await self._client.fetch_events(
            ReqTarget.auto([filter_builder]), timedelta(seconds=10))

        # Deduplicate by d-tag keeping the latest event (parameterized replaceable).
        # Use content timestamp so close() (which bumps timestamp by 1) always wins
        # over the original even when both Nostr events share the same created_at second.
        latest: dict[str, tuple[int, BountyAnnouncement]] = {}
        for event in events:
            try:
                c = BountyAnnouncement.from_json(event.content())
                d = c.d_tag()
                ts = c.timestamp  # content timestamp, not Nostr event created_at
                if d not in latest or ts > latest[d][0]:
                    latest[d] = (ts, c)
            except Exception as e:
                print(f"Error parsing bounty announcement: {e}")

        bounties = [c for _, c in latest.values()]
        if active_only:
            bounties = [c for c in bounties if c.active]
        return bounties

    async def submit_prediction(
        self,
        stream_name: str,
        stream_provider_pubkey: str,
        host_pubkey: str,
        seq_num: int,
        predicted_value: float,
        predictor_alias_pubkey: str,
    ) -> str:
        """Submit a prediction to a bounty host (predictor).

        Sends an encrypted DM (kind 34608, NIP-04) directly to the host.
        Not broadcast publicly — only the host can decrypt it.

        Args:
            stream_name: Stream being predicted
            stream_provider_pubkey: Producer of the stream
            host_pubkey: Bounty host's Nostr pubkey
            seq_num: Observation sequence number being predicted
            predicted_value: The predictor's prediction
            predictor_alias_pubkey: The predictor's alias pubkey, so the
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
            predictor_alias_pubkey=predictor_alias_pubkey,
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
        output = await self._send_builder(builder)
        return output.id().to_hex()

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
        """Publish an observation (provider).

        A free stream: one public 34601 with the observation. A paid stream:
        its public heartbeat (d=stream_name, seq and timestamp in tags, no
        value) on every observation; the value goes to each paying buyer
        with deliver_observation(), which the market decides.

        Args:
            observation: The observation to publish
            stream_metadata: Metadata about this stream (for pricing info)

        Returns:
            List of event IDs

        Raises:
            RuntimeError: If client not running
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")

        stream_name = observation.stream_name
        seq_num = observation.seq_num

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

            output = await self._send_builder(builder)
            event_ids.append(output.id().to_hex())
            self._stats["observations_sent"] += 1
        else:
            # Paid stream: the public heartbeat, tags only, no value, so the
            # relay always has a fresh created_at for marketplace freshness.
            tags = [
                Tag.parse(["d", stream_name]),
                Tag.parse(["stream", stream_name]),
                Tag.parse(["seq", str(seq_num)]),
                Tag.parse(["timestamp", str(observation.timestamp)]),
                Tag.parse(["satori", "heartbeat"]),
            ]
            builder = EventBuilder(Kind(KIND_DATASTREAM_DATA), "").tags(tags)
            output = await self._send_builder(builder)
            event_ids.append(output.id().to_hex())

        return event_ids

    async def deliver_observation(
        self,
        observation: DatastreamObservation,
        subscriber_pubkey: str,
    ) -> str | None:
        """Deliver a paid stream's observation to one buyer (provider).

        One 34601 per buyer: d=<stream>:<buyer> (a relay keeps one event per
        d, so a shared d would leave only one buyer's), p=buyer, seq and
        timestamp in tags, and the value NIP-04 encrypted to the buyer.

        Returns the event ID when a relay took it, else None.
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")
        stream_name = observation.stream_name
        encrypted_value = encrypt_observation(
            str(observation.value), PublicKey.parse(subscriber_pubkey),
            self._keys)
        tags = [
            Tag.parse(["d", f"{stream_name}:{subscriber_pubkey}"]),
            Tag.parse(["p", subscriber_pubkey]),
            Tag.parse(["stream", stream_name]),
            Tag.parse(["seq", str(observation.seq_num)]),
            Tag.parse(["timestamp", str(observation.timestamp)]),
            Tag.parse(["satori", "observation"]),
        ]
        builder = EventBuilder(
            Kind(KIND_DATASTREAM_DATA), encrypted_value).tags(tags)
        signed = self._keys.sign_event(
            builder.finalize_unsigned(self._keys.public_key()))
        output = await self._client.send_event(signed)
        if not output.success:
            return None
        self._stats["observations_sent"] += 1
        return signed.id().to_hex()

    # ========================================================================
    # SUBSCRIBER APIs
    # ========================================================================

    async def discover_datastreams(
        self,
        tags: list[str] | None = None,
        limit: int | None = 100
    ) -> list[DatastreamMetadata]:
        """Discover available datastreams (subscriber).

        Queries relays for datastream announcements (kind 34600), paging
        backwards in time with `until`. Relays clamp each REQ to their own
        max limit (strfry `maxFilterLimit`, 500 on Satori relays), so a
        single REQ silently misses everything past the newest page.

        Args:
            tags: Optional list of tags to filter by
            limit: Maximum number of results (None = no cap)

        Returns:
            List of datastream metadata

        Raises:
            RuntimeError: If client not running
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")

        seen_ids: set[str] = set()
        seen_coords: set[tuple[str, str]] = set()
        datastreams = []
        until = None
        stepped = False
        while True:
            filter_builder = Filter().kind(
                Kind(KIND_DATASTREAM_ANNOUNCE)).limit(DISCOVERY_PAGE_SIZE)
            if tags:
                for tag in tags:
                    filter_builder = filter_builder.hashtag(tag)
            if until is not None:
                filter_builder = filter_builder.until(Timestamp.from_secs(until))

            events = await self._client.fetch_events(
                ReqTarget.auto([filter_builder]), timedelta(seconds=10))
            new = [e for e in events if e.id().to_hex() not in seen_ids]
            if not new:
                # A page of only repeats means the boundary second is full;
                # step past it once, then stop.
                if until is None or stepped:
                    break
                until -= 1
                stepped = True
                continue
            stepped = False

            # Newest first, so the first event seen per (author, d) wins.
            new.sort(key=lambda e: e.created_at().as_secs(), reverse=True)
            for event in new:
                seen_ids.add(event.id().to_hex())
                coord = (event.author().to_hex(), _d_tag(event))
                if coord in seen_coords:
                    continue
                seen_coords.add(coord)
                try:
                    # Skip events with status=deleted tag (tombstones)
                    tag_values = [t.to_vec() for t in event.tags()]
                    if ["status", "deleted"] in tag_values:
                        continue
                    datastreams.append(
                        DatastreamMetadata.from_json(event.content()))
                except Exception as e:
                    print(f"Error parsing datastream metadata: {e}")
                if limit and len(datastreams) >= limit:
                    return datastreams
            until = min(e.created_at().as_secs() for e in new)

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

        events = await self._client.fetch_events(
            ReqTarget.auto([filter_builder]), timedelta(seconds=10))

        if not events:
            return None
        return self.parse_observation_event(events[0])

    def parse_observation_event(self, event: Event) -> InboundObservation | None:
        """Parse a kind 34601 event fetched from a relay.

        Returns None for heartbeats and for events this client can't decrypt.
        """
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

    async def _fetch_latest_by_coordinate(
        self, kind: int, pairs: list[tuple[str, str]]
    ) -> dict[tuple[str, str], Event]:
        """Fetch the newest event of `kind` for each (author, d-tag) pair.

        Builds one filter per author (authors=[a], identifiers=its names)
        and packs up to FILTER_CHUNK_SIZE filters into each REQ, instead
        of one REQ per stream.
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")
        by_author: dict[str, list[str]] = {}
        for author, name in dict.fromkeys(pairs):
            by_author.setdefault(author, []).append(name)
        filters = []
        for author, names in by_author.items():
            pubkey = PublicKey.parse(author)
            for i in range(0, len(names), DISCOVERY_PAGE_SIZE):
                chunk = names[i:i + DISCOVERY_PAGE_SIZE]
                filters.append(
                    Filter()
                    .kind(Kind(kind))
                    .authors([pubkey])
                    .identifiers(chunk)
                    .limit(len(chunk)))
        wanted = set(pairs)
        latest: dict[tuple[str, str], Event] = {}
        for i in range(0, len(filters), FILTER_CHUNK_SIZE):
            events = await self._client.fetch_events(
                ReqTarget.auto(filters[i:i + FILTER_CHUNK_SIZE]),
                timedelta(seconds=10))
            for event in events:
                key = (event.author().to_hex(), _d_tag(event))
                if key not in wanted:
                    continue
                prev = latest.get(key)
                if (prev is None or event.created_at().as_secs()
                        > prev.created_at().as_secs()):
                    latest[key] = event
        return latest

    async def get_last_observation_events(
        self, pairs: list[tuple[str, str]]
    ) -> dict[tuple[str, str], Event]:
        """Latest observation event (kind 34601) per (provider_pubkey,
        stream_name), batched. Read `created_at` for freshness (works for
        paid streams too) or pass the event to parse_observation_event()."""
        return await self._fetch_latest_by_coordinate(
            KIND_DATASTREAM_DATA, pairs)

    async def find_datastreams(
        self, pairs: list[tuple[str, str]]
    ) -> dict[tuple[str, str], DatastreamMetadata]:
        """Look up specific datastream announcements by (provider_pubkey,
        stream_name) without downloading the relay's whole catalog.
        Tombstoned streams are omitted."""
        found = {}
        latest = await self._fetch_latest_by_coordinate(
            KIND_DATASTREAM_ANNOUNCE, pairs)
        for key, event in latest.items():
            try:
                tag_values = [t.to_vec() for t in event.tags()]
                if ["status", "deleted"] in tag_values:
                    continue
                found[key] = DatastreamMetadata.from_json(event.content())
            except Exception as e:
                print(f"Error parsing datastream metadata: {e}")
        return found

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
        events = await self._client.fetch_events(
            ReqTarget.auto([filter_builder]), timedelta(seconds=10))
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
        payment_channel: dict | None = None
    ) -> str:
        """Subscribe to a datastream (subscriber).

        Publishes a subscription announcement (kind 34602, public).

        Args:
            stream_name: Stream identifier to subscribe to
            provider_pubkey: Provider's public key (hex)
            payment_channel: A paid stream's channel on Base:
                {chain, contract, id, binding}

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

        output = await self._send_builder(builder)

        # Update statistics
        self._stats["subscriptions_announced"] += 1

        return output.id().to_hex()

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
        output = await self._send_builder(builder)
        return output.id().to_hex()

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
        voucher: dict | None = None
    ) -> str:
        """Send payment notification to provider (subscriber).

        Sends encrypted DM (kind 34603) to provider.

        Args:
            provider_pubkey: Provider's public key (hex)
            stream_name: Stream identifier
            seq_num: Observation sequence number being paid for
            amount_sats: Payment amount in satoshis (the price)
            voucher: The alias's cumulative voucher on the channel:
                {channel, cumulative (wei, decimal string), signature}

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
            voucher=voucher,
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

        output = await self._send_builder(builder)

        # Update statistics
        self._stats["payments_sent"] += 1

        return output.id().to_hex()

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

        Note: `commitment.receiver_pubkey` is the 33-byte alias pubkey used
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

        output = await self._send_builder(builder)
        self._stats["commitments_sent"] += 1
        return output.id().to_hex()

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

        events = await self._client.fetch_events(
            ReqTarget.auto([filter_obj]), timedelta(seconds=10))

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

        output = await self._send_builder(builder)
        return output.id().to_hex()

    async def publish_channel_open(
        self,
        channel_open: ChannelOpen,
        receiver_nostr_pubkey: str,
    ) -> str:
        """Announce a new channel to the receiver via Nostr (sender side).

        Publishes a kind 34605 parameterized replaceable event so the receiver
        can save the channel to their DB and process future commitments.

        Args:
            channel_open: Channel metadata (alias pubkeys, redeem script, etc.)
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

        output = await self._send_builder(builder)
        return output.id().to_hex()

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
        output = await self._send_builder(builder)
        return output.id().to_hex()

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

    async def subscriptions(self) -> AsyncIterator[InboundSubscription]:
        """Receive subscriptions and unsubscriptions to our streams (provider).

        Yields:
            InboundSubscription instances as they arrive

        Raises:
            RuntimeError: If client not running
        """
        if not self._running:
            raise RuntimeError("Client not running")

        while self._running:
            try:
                sub = await asyncio.wait_for(
                    self._subscription_queue.get(), timeout=1.0)
                yield sub
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

        events = await self._client.fetch_events(
            ReqTarget.auto([filter_obj]), timedelta(seconds=10))

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

        output = await self._send_builder(builder)

        return output.id().to_hex()

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

    # ========================================================================
    # INTERNAL - Event Processing
    # ========================================================================

    async def set_followed_authors(self, pubkeys: list[str]) -> None:
        """Follow public events from these authors: observations (34601)
        and commitment tombstones (34604, which carry no `p` tag).

        Pass the providers we subscribe to plus the receivers of channels we
        fund. Replaces the previous set; an empty list stops following.
        """
        if not self._running or not self._client:
            raise RuntimeError("Client not running")
        pubkeys = sorted(set(pubkeys))
        if pubkeys == self._followed_authors:
            return
        self._followed_authors = pubkeys
        try:
            await self._client.unsubscribe(FOLLOW_SUBSCRIPTION_ID)
        except Exception:
            pass  # nothing to close yet
        if not pubkeys:
            return
        filters = [
            Filter()
            .kinds([Kind(KIND_DATASTREAM_DATA), Kind(KIND_CHANNEL_COMMITMENT)])
            .authors([PublicKey.parse(p)
                      for p in pubkeys[i:i + FILTER_CHUNK_SIZE]])
            for i in range(0, len(pubkeys), FILTER_CHUNK_SIZE)
        ]
        await self._client.subscribe(
            ReqTarget.auto(filters), id=FOLLOW_SUBSCRIPTION_ID)

    async def _subscribe_defaults(self) -> None:
        """Subscribe to Satori events addressed to us, plus (until
        set_followed_authors() narrows it) all public observations and
        commitment tombstones."""
        # Events addressed to us carry our pubkey in a `p` tag.
        personal_filter = Filter().kinds([
            Kind(KIND_DATASTREAM_DATA),
            Kind(KIND_SUBSCRIPTION_ANNOUNCE),
            Kind(KIND_PAYMENT),
            Kind(KIND_CHANNEL_COMMITMENT),
            Kind(KIND_CHANNEL_OPEN),
            Kind(KIND_CHANNEL_SETTLED),
            Kind(KIND_PREDICTION),
            Kind(KIND_ACCESS_REQUEST),
        ]).pubkey(self._keys.public_key())
        # 0.45 takes a ReqTarget rather than a bare Filter, so a subscription can
        # say WHICH relays it is for. `auto` keeps the previous behaviour: send
        # it to every connected relay.
        await self._client.subscribe(ReqTarget.auto([personal_filter]))
        # Public events (free observations, commitment tombstones) have no
        # `p` tag. Default: all of them, as before. Callers that know whom
        # they follow should narrow this with set_followed_authors() so the
        # connection doesn't pull the relay's whole observation firehose.
        public_filter = Filter().kinds([
            Kind(KIND_DATASTREAM_DATA),
            Kind(KIND_CHANNEL_COMMITMENT),
        ])
        await self._client.subscribe(
            ReqTarget.auto([public_filter]), id=FOLLOW_SUBSCRIPTION_ID)

    async def _event_listener(self, notifications) -> None:
        """Background task that drains incoming Nostr events."""
        # Drain the notification stream.
        #
        # nostr-sdk 0.45 replaced the push-style `handle_notifications(handler)`
        # and its `HandleNotification` base class with a pull-style stream:
        # `client.notifications()` yields `ClientNotification` variants and we
        # iterate them. The three values the old callback received
        # (relay_url, subscription_id, event) are the fields of NEW_EVENT, so
        # only the plumbing changed, not the information.
        try:
            while True:
                notification = await notifications.next()
                if notification is None:
                    break
                if notification.is_shutdown():
                    break
                if notification.is_new_event():
                    await self._handle_event(notification.event)
                # MESSAGE carries relay protocol traffic (OK/EOSE/NOTICE), which
                # this client does not act on. Ignored as before.
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
            for tag in event.tags():
                vec = tag.to_vec()
                if len(vec) >= 2:
                    tag_map[vec[0]] = vec[1]

            # Try plaintext JSON first (free stream)
            paid = False
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
                paid = p_tag == self._keys.public_key().to_hex()

            inbound = InboundObservation(
                stream_name=observation.stream_name,
                nostr_pubkey=sender_pubkey.to_hex(),
                observation=observation,
                event_id=event.id().to_hex(),
                paid=paid,
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
            # Only the payer itself can say it paid.
            if payment.from_pubkey != sender_pubkey.to_hex():
                return

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

        A (un)subscription to one of our streams, sent by the subscriber
        itself, is queued for subscriptions(); an action=unsubscribe tag
        marks an unsubscription.
        """
        try:
            # Check for unsubscribe action tag
            action = None
            for tag in event.tags():
                vec = tag.to_vec()
                if len(vec) >= 2 and vec[0] == 'action':
                    action = vec[1]
                    break

            # Parse subscription (public content)
            sub = SubscriptionAnnouncement.from_json(event.content())

            if (sub.nostr_pubkey == self.pubkey()
                    and sub.subscriber_pubkey == event.author().to_hex()):
                await self._subscription_queue.put(InboundSubscription(
                    subscription=sub,
                    unsubscribe=action == 'unsubscribe',
                    event_id=event.id().to_hex(),
                ))

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

        The payload's `receiver_pubkey` is a 33-byte alias pubkey and
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
                    for tag in event.tags():
                        tag_vec = tag.to_vec()
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
            for tag in event.tags():
                tag_vec = tag.to_vec()
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
        in start.py) filters to those addressed to this node's alias pubkey.
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
