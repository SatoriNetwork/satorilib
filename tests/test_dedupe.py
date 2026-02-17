"""Tests for satori_nostr deduplication cache.

Covers: DedupeCache (is_seen, mark_seen, size, clear, LRU eviction)
"""
import pytest

from satorilib.satori_nostr.dedupe import DedupeCache


class TestDedupeCache:
    def test_new_event_not_seen(self):
        cache = DedupeCache()
        assert cache.is_seen("event1") is False

    def test_mark_then_seen(self):
        cache = DedupeCache()
        cache.mark_seen("event1")
        assert cache.is_seen("event1") is True

    def test_different_events_independent(self):
        cache = DedupeCache()
        cache.mark_seen("event1")
        assert cache.is_seen("event2") is False

    def test_size(self):
        cache = DedupeCache()
        assert cache.size() == 0
        cache.mark_seen("e1")
        cache.mark_seen("e2")
        assert cache.size() == 2

    def test_size_no_duplicates(self):
        cache = DedupeCache()
        cache.mark_seen("e1")
        cache.mark_seen("e1")
        assert cache.size() == 1

    def test_clear(self):
        cache = DedupeCache()
        cache.mark_seen("e1")
        cache.mark_seen("e2")
        cache.clear()
        assert cache.size() == 0
        assert cache.is_seen("e1") is False

    def test_lru_eviction(self):
        cache = DedupeCache(max_size=3)
        cache.mark_seen("e1")
        cache.mark_seen("e2")
        cache.mark_seen("e3")
        # Cache full, adding e4 should evict e1 (oldest)
        cache.mark_seen("e4")
        assert cache.size() == 3
        assert cache.is_seen("e1") is False
        assert cache.is_seen("e2") is True
        assert cache.is_seen("e4") is True

    def test_lru_access_refreshes(self):
        cache = DedupeCache(max_size=3)
        cache.mark_seen("e1")
        cache.mark_seen("e2")
        cache.mark_seen("e3")
        # Access e1, making it most recent
        cache.is_seen("e1")
        # Now add e4 — should evict e2 (oldest after e1 was refreshed)
        cache.mark_seen("e4")
        assert cache.is_seen("e1") is True
        assert cache.is_seen("e2") is False

    def test_mark_seen_refreshes(self):
        cache = DedupeCache(max_size=3)
        cache.mark_seen("e1")
        cache.mark_seen("e2")
        cache.mark_seen("e3")
        # Re-mark e1
        cache.mark_seen("e1")
        # Add e4 — should evict e2
        cache.mark_seen("e4")
        assert cache.is_seen("e1") is True
        assert cache.is_seen("e2") is False

    def test_default_max_size(self):
        cache = DedupeCache()
        # Default is 50000, just verify it doesn't crash
        for i in range(100):
            cache.mark_seen(f"event_{i}")
        assert cache.size() == 100
