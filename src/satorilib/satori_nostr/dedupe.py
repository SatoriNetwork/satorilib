"""Event deduplication for Nostr messages.

Prevents processing the same event multiple times by tracking seen event IDs.
"""
from collections import OrderedDict
from typing import Optional


class DedupeCache:
    """In-memory LRU cache for event deduplication.

    Tracks seen event IDs with automatic eviction of oldest entries
    when cache reaches max_size.

    Args:
        max_size: Maximum number of event IDs to track (default: 50000)
    """

    def __init__(self, max_size: int = 50000):
        """Initialize dedupe cache.

        Args:
            max_size: Maximum number of events to track before evicting oldest
        """
        self._max_size = max_size
        self._cache: OrderedDict[str, bool] = OrderedDict()

    def is_seen(self, event_id: str) -> bool:
        """Check if an event has been seen before.

        This method also updates the LRU order (moves accessed item to end).

        Args:
            event_id: The Nostr event ID to check

        Returns:
            True if event was previously marked as seen, False otherwise
        """
        if event_id in self._cache:
            # Move to end (most recently used)
            self._cache.move_to_end(event_id)
            return True
        return False

    def mark_seen(self, event_id: str) -> None:
        """Mark an event as seen.

        Adds the event ID to the cache. If cache is full, evicts the
        least recently used (oldest) entry.

        Args:
            event_id: The Nostr event ID to mark as seen
        """
        # If already exists, move to end
        if event_id in self._cache:
            self._cache.move_to_end(event_id)
            return

        # Add new entry
        self._cache[event_id] = True

        # Evict oldest if over max_size
        if len(self._cache) > self._max_size:
            self._cache.popitem(last=False)  # Remove oldest (first item)

    def size(self) -> int:
        """Get current number of tracked events.

        Returns:
            Number of event IDs currently in cache
        """
        return len(self._cache)

    def clear(self) -> None:
        """Clear all entries from the cache."""
        self._cache.clear()


# Optional: SQLite-backed deduplication (future enhancement)
class SQLiteDedupe:
    """SQLite-backed event deduplication with persistence.

    Not implemented in MVP - placeholder for future enhancement.
    """

    def __init__(self, db_path: str):
        """Initialize SQLite dedupe store.

        Args:
            db_path: Path to SQLite database file
        """
        raise NotImplementedError(
            "SQLite deduplication not yet implemented. Use DedupeCache for now."
        )
