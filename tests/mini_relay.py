"""Minimal in-process Nostr relay for testing.

Implements just enough of NIP-01 to support the SatoriNostr test suite:
- EVENT: stores events in memory, pushes to live subscribers
- REQ: queries stored events by filter, starts live subscription
- CLOSE: closes subscriptions

Runs as an asyncio task on localhost with a random free port.
"""
import asyncio
import json
import time
import hashlib
from websockets.asyncio.server import serve


class MiniRelay:
    """Minimal NIP-01 Nostr relay for testing."""

    def __init__(self, debug: bool = False):
        self.events: list[dict] = []  # stored events
        # ws_id -> {sub_id: {"filters": [...], "ws": websocket}}
        self._live_subs: dict[str, dict] = {}
        self._server = None
        self._port = None
        self._ws_counter = 0
        self._debug = debug
        self._ws_map: dict[str, object] = {}  # ws_id -> websocket

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self._port}"

    async def start(self, port: int = 0):
        """Start the relay on the given port (0 = random free port)."""
        self._server = await serve(
            self._handler, "127.0.0.1", port)
        self._port = self._server.sockets[0].getsockname()[1]

    async def stop(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    def clear(self):
        """Clear all stored events."""
        self.events.clear()
        self._live_subs.clear()
        self._ws_map.clear()

    async def _handler(self, websocket):
        ws_id = f"ws_{self._ws_counter}"
        self._ws_counter += 1
        self._live_subs[ws_id] = {}
        self._ws_map[ws_id] = websocket

        try:
            async for message in websocket:
                try:
                    msg = json.loads(message)
                except json.JSONDecodeError:
                    await websocket.send(json.dumps(
                        ["NOTICE", "invalid JSON"]))
                    continue

                if not isinstance(msg, list) or len(msg) < 2:
                    continue

                msg_type = msg[0]

                if self._debug:
                    print(f"[relay {ws_id}] recv: {msg_type} {str(msg)[:200]}")

                if msg_type == "EVENT":
                    await self._handle_event(websocket, ws_id, msg)
                elif msg_type == "REQ":
                    await self._handle_req(websocket, ws_id, msg)
                elif msg_type == "CLOSE":
                    sub_id = msg[1] if len(msg) > 1 else ""
                    if ws_id in self._live_subs:
                        self._live_subs[ws_id].pop(sub_id, None)
                    await websocket.send(json.dumps(
                        ["CLOSED", sub_id, ""]))
        finally:
            self._live_subs.pop(ws_id, None)
            self._ws_map.pop(ws_id, None)

    async def _handle_event(self, websocket, ws_id, msg):
        if len(msg) < 2:
            return
        event = msg[1]
        event_id = event.get("id", "")

        # Store
        self.events.append(event)

        # OK response
        await websocket.send(json.dumps(
            ["OK", event_id, True, ""]))

        # Push to all live subscribers whose filters match
        for other_ws_id, subs in list(self._live_subs.items()):
            other_ws = self._ws_map.get(other_ws_id)
            if other_ws is None:
                continue
            for sub_id, filters in subs.items():
                if self._matches_any_filter(event, filters):
                    try:
                        await other_ws.send(json.dumps(
                            ["EVENT", sub_id, event]))
                    except Exception:
                        pass  # connection may have closed

    async def _handle_req(self, websocket, ws_id, msg):
        if len(msg) < 3:
            return
        sub_id = msg[1]
        filters = msg[2:]  # can be multiple filters

        # Register live subscription
        if ws_id not in self._live_subs:
            self._live_subs[ws_id] = {}
        self._live_subs[ws_id][sub_id] = filters

        # Send matching stored events
        for event in self.events:
            if self._matches_any_filter(event, filters):
                await websocket.send(json.dumps(
                    ["EVENT", sub_id, event]))

        # Send EOSE
        await websocket.send(json.dumps(["EOSE", sub_id]))

    def _matches_any_filter(self, event: dict, filters: list[dict]) -> bool:
        return any(self._matches_filter(event, f) for f in filters)

    def _matches_filter(self, event: dict, filt: dict) -> bool:
        # Check kinds
        if "kinds" in filt:
            if event.get("kind") not in filt["kinds"]:
                return False

        # Check ids
        if "ids" in filt:
            if event.get("id") not in filt["ids"]:
                return False

        # Check authors
        if "authors" in filt:
            if event.get("pubkey") not in filt["authors"]:
                return False

        # Check since/until
        if "since" in filt:
            if event.get("created_at", 0) < filt["since"]:
                return False
        if "until" in filt:
            if event.get("created_at", 0) > filt["until"]:
                return False

        # Check tag filters (#e, #p, #t, #d, etc.)
        event_tags = event.get("tags", [])
        for key, values in filt.items():
            if key.startswith("#") and len(key) == 2:
                tag_letter = key[1]
                # Get all values for this tag from the event
                event_tag_values = [
                    t[1] for t in event_tags
                    if len(t) >= 2 and t[0] == tag_letter
                ]
                # At least one filter value must match
                if not any(v in event_tag_values for v in values):
                    return False

        # Check limit (handled at query level, not per-event)
        return True
