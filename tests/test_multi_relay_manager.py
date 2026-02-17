"""Tests for MultiRelayManager integration.

Covers: relay status tracking, event deduplication, health classification,
        best relay selection, circuit breaker, reconnect logic, statistics.
"""
import asyncio
import time
import pytest

from satorilib.satori_nostr.integrations.multi_relay_manager import (
    MultiRelayManager,
    RelayStatus,
)


# =========================================================================
# RelayStatus dataclass
# =========================================================================

class TestRelayStatus:
    def test_defaults(self):
        s = RelayStatus(url="wss://r1.com")
        assert s.url == "wss://r1.com"
        assert s.connected is False
        assert s.last_event_time == 0
        assert s.error_count == 0
        assert s.last_error is None
        assert s.connection_time == 0


# =========================================================================
# Construction
# =========================================================================

class TestConstruction:
    def test_create(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com", "wss://r2.com"])
        assert len(m.relay_urls) == 2
        assert m.min_active_relays == 1
        assert m.max_error_count == 5
        assert m.reconnect_delay == 30

    def test_custom_params(self):
        m = MultiRelayManager(
            relay_urls=["wss://r1.com"],
            min_active_relays=3,
            max_error_count=10,
            reconnect_delay=60,
        )
        assert m.min_active_relays == 3
        assert m.max_error_count == 10
        assert m.reconnect_delay == 60

    def test_initial_status(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com", "wss://r2.com"])
        status = m.get_all_relay_status()
        assert len(status) == 2
        assert all(not s.connected for s in status.values())

    def test_initial_stats(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com"])
        stats = m.get_statistics()
        assert stats["events_received"] == 0
        assert stats["events_deduplicated"] == 0
        assert stats["relay_failures"] == 0


# =========================================================================
# Relay status tracking
# =========================================================================

class TestRelayTracking:
    def test_mark_connected(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com"])
        m.mark_relay_connected("wss://r1.com")
        status = m.get_relay_status("wss://r1.com")
        assert status.connected is True
        assert status.error_count == 0
        assert status.connection_time > 0

    def test_mark_connected_resets_errors(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com"])
        m.mark_relay_disconnected("wss://r1.com", "test error")
        m.mark_relay_disconnected("wss://r1.com", "another error")
        assert m.get_relay_status("wss://r1.com").error_count == 2
        m.mark_relay_connected("wss://r1.com")
        assert m.get_relay_status("wss://r1.com").error_count == 0

    def test_mark_disconnected(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com"])
        m.mark_relay_connected("wss://r1.com")
        m.mark_relay_disconnected("wss://r1.com", "connection lost")
        status = m.get_relay_status("wss://r1.com")
        assert status.connected is False
        assert status.error_count == 1
        assert status.last_error == "connection lost"

    def test_disconnect_increments_errors(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com"])
        m.mark_relay_disconnected("wss://r1.com", "err1")
        m.mark_relay_disconnected("wss://r1.com", "err2")
        m.mark_relay_disconnected("wss://r1.com", "err3")
        assert m.get_relay_status("wss://r1.com").error_count == 3

    def test_disconnect_increments_failure_stat(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com"])
        m.mark_relay_disconnected("wss://r1.com")
        m.mark_relay_disconnected("wss://r1.com")
        assert m.get_statistics()["relay_failures"] == 2

    def test_mark_unknown_relay_noop(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com"])
        m.mark_relay_connected("wss://unknown.com")  # should not crash
        m.mark_relay_disconnected("wss://unknown.com")
        assert m.get_relay_status("wss://unknown.com") is None

    def test_get_relay_status_nonexistent(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com"])
        assert m.get_relay_status("wss://nope.com") is None


# =========================================================================
# Event deduplication
# =========================================================================

class TestDeduplication:
    def test_new_event(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com"])
        m.mark_relay_connected("wss://r1.com")
        assert m.mark_event_received("wss://r1.com", "event1") is True

    def test_duplicate_event(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com"])
        m.mark_relay_connected("wss://r1.com")
        m.mark_event_received("wss://r1.com", "event1")
        assert m.mark_event_received("wss://r1.com", "event1") is False

    def test_duplicate_across_relays(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com", "wss://r2.com"])
        m.mark_relay_connected("wss://r1.com")
        m.mark_relay_connected("wss://r2.com")
        assert m.mark_event_received("wss://r1.com", "event1") is True
        assert m.mark_event_received("wss://r2.com", "event1") is False

    def test_different_events(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com"])
        m.mark_relay_connected("wss://r1.com")
        assert m.mark_event_received("wss://r1.com", "event1") is True
        assert m.mark_event_received("wss://r1.com", "event2") is True

    def test_is_duplicate(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com"])
        assert m.is_duplicate("event1") is False
        m.add_event("event1")
        assert m.is_duplicate("event1") is True

    def test_event_updates_relay_last_event_time(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com"])
        m.mark_relay_connected("wss://r1.com")
        m.mark_event_received("wss://r1.com", "event1")
        status = m.get_relay_status("wss://r1.com")
        assert status.last_event_time > 0

    def test_dedup_stats(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com", "wss://r2.com"])
        m.mark_event_received("wss://r1.com", "e1")
        m.mark_event_received("wss://r2.com", "e1")  # dup
        m.mark_event_received("wss://r1.com", "e2")
        stats = m.get_statistics()
        assert stats["events_received"] == 2
        assert stats["events_deduplicated"] == 1


# =========================================================================
# Health classification
# =========================================================================

class TestHealthClassification:
    def test_healthy_relays(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com", "wss://r2.com"])
        m.mark_relay_connected("wss://r1.com")
        assert m.get_healthy_relays() == ["wss://r1.com"]

    def test_all_healthy(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com", "wss://r2.com"])
        m.mark_relay_connected("wss://r1.com")
        m.mark_relay_connected("wss://r2.com")
        assert len(m.get_healthy_relays()) == 2

    def test_unhealthy_not_connected(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com", "wss://r2.com"])
        assert len(m.get_unhealthy_relays()) == 2

    def test_unhealthy_too_many_errors(self):
        m = MultiRelayManager(
            relay_urls=["wss://r1.com"], max_error_count=3)
        m.mark_relay_connected("wss://r1.com")
        for _ in range(3):
            m.mark_relay_disconnected("wss://r1.com")
        m.mark_relay_connected("wss://r1.com")
        # error_count resets on connect
        assert m.get_healthy_relays() == ["wss://r1.com"]

    def test_error_threshold_marks_unhealthy(self):
        m = MultiRelayManager(
            relay_urls=["wss://r1.com"], max_error_count=2)
        m.mark_relay_connected("wss://r1.com")
        # Still connected but errors >= max
        status = m.get_relay_status("wss://r1.com")
        status.error_count = 2
        assert m.get_healthy_relays() == []

    def test_needs_more_relays(self):
        m = MultiRelayManager(
            relay_urls=["wss://r1.com", "wss://r2.com"],
            min_active_relays=2)
        m.mark_relay_connected("wss://r1.com")
        assert m.needs_more_relays() is True
        m.mark_relay_connected("wss://r2.com")
        assert m.needs_more_relays() is False


# =========================================================================
# Best relay selection
# =========================================================================

class TestBestRelay:
    def test_no_healthy(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com"])
        assert m.get_best_relay() is None

    def test_single_healthy(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com", "wss://r2.com"])
        m.mark_relay_connected("wss://r1.com")
        assert m.get_best_relay() == "wss://r1.com"

    def test_prefers_lower_errors(self):
        m = MultiRelayManager(
            relay_urls=["wss://r1.com", "wss://r2.com"],
            max_error_count=10)
        m.mark_relay_connected("wss://r1.com")
        m.mark_relay_connected("wss://r2.com")
        # Give r1 some errors (but still below threshold)
        s1 = m.get_relay_status("wss://r1.com")
        s1.error_count = 3
        assert m.get_best_relay() == "wss://r2.com"

    def test_prefers_recent_events(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com", "wss://r2.com"])
        m.mark_relay_connected("wss://r1.com")
        m.mark_relay_connected("wss://r2.com")
        # Both zero errors, r2 has more recent event
        s1 = m.get_relay_status("wss://r1.com")
        s2 = m.get_relay_status("wss://r2.com")
        s1.last_event_time = 1000
        s2.last_event_time = 2000
        assert m.get_best_relay() == "wss://r2.com"


# =========================================================================
# Reconnect logic
# =========================================================================

class TestReconnectLogic:
    def test_should_reconnect_disconnected(self):
        m = MultiRelayManager(
            relay_urls=["wss://r1.com"], reconnect_delay=0)
        m.mark_relay_disconnected("wss://r1.com")
        assert m.should_reconnect_relay("wss://r1.com") is True

    def test_should_not_reconnect_connected(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com"])
        m.mark_relay_connected("wss://r1.com")
        assert m.should_reconnect_relay("wss://r1.com") is False

    def test_should_not_reconnect_too_many_errors(self):
        m = MultiRelayManager(
            relay_urls=["wss://r1.com"],
            max_error_count=3,
            reconnect_delay=0)
        for _ in range(3):
            m.mark_relay_disconnected("wss://r1.com")
        assert m.should_reconnect_relay("wss://r1.com") is False

    def test_should_not_reconnect_too_soon(self):
        m = MultiRelayManager(
            relay_urls=["wss://r1.com"], reconnect_delay=9999)
        m.mark_relay_connected("wss://r1.com")
        m.mark_relay_disconnected("wss://r1.com")
        assert m.should_reconnect_relay("wss://r1.com") is False

    def test_should_reconnect_unknown_relay(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com"])
        assert m.should_reconnect_relay("wss://unknown.com") is False


# =========================================================================
# Lifecycle
# =========================================================================

class TestLifecycle:
    @pytest.mark.asyncio
    async def test_start_stop(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com"])
        await m.start()
        assert m._running is True
        await m.stop()
        assert m._running is False

    @pytest.mark.asyncio
    async def test_double_start_raises(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com"])
        await m.start()
        try:
            with pytest.raises(RuntimeError):
                await m.start()
        finally:
            await m.stop()

    @pytest.mark.asyncio
    async def test_stop_when_not_running(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com"])
        await m.stop()  # should not raise


# =========================================================================
# Statistics
# =========================================================================

class TestStatistics:
    def test_stats_include_relay_counts(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com", "wss://r2.com"])
        m.mark_relay_connected("wss://r1.com")
        stats = m.get_statistics()
        assert stats["healthy_relays"] == 1
        assert stats["unhealthy_relays"] == 1

    def test_get_all_relay_status_is_copy(self):
        m = MultiRelayManager(relay_urls=["wss://r1.com"])
        s1 = m.get_all_relay_status()
        s2 = m.get_all_relay_status()
        assert s1 is not s2
