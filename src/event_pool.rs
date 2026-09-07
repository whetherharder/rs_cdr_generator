// Object pool for EventRow to eliminate allocations in hot paths
use crate::writer::EventRow;

/// Simple ring buffer pool for EventRow objects
/// Each worker thread has its own pool to avoid synchronization overhead
pub struct EventPool {
    pool: Vec<EventRow>,
    next: usize,
    capacity: usize,
}

impl EventPool {
    /// Create a new event pool with the specified capacity
    /// Pre-allocates all EventRow objects upfront
    pub fn new(capacity: usize) -> Self {
        let pool: Vec<EventRow> = (0..capacity)
            .map(|_| EventRow::default())
            .collect();

        EventPool {
            pool,
            next: 0,
            capacity,
        }
    }

    /// Acquire an EventRow from the pool
    /// Returns a mutable reference to a reset EventRow
    /// Uses ring buffer approach - wraps around when reaching capacity
    pub fn acquire(&mut self) -> &mut EventRow {
        let event = &mut self.pool[self.next];
        self.next = (self.next + 1) % self.capacity;
        event.reset();
        event
    }

    /// Get the current capacity of the pool
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get statistics about pool usage (for debugging/monitoring)
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            capacity: self.capacity,
            current_index: self.next,
        }
    }
}

/// Statistics about pool usage
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub capacity: usize,
    pub current_index: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_pool_creation() {
        let pool = EventPool::new(100);
        assert_eq!(pool.capacity(), 100);
        assert_eq!(pool.stats().current_index, 0);
    }

    #[test]
    fn test_event_pool_acquire() {
        let mut pool = EventPool::new(10);

        // Acquire first event
        let event1 = pool.acquire();
        event1.calling_number = "123456".to_string();
        assert_eq!(pool.stats().current_index, 1);

        // Acquire second event
        let event2 = pool.acquire();
        event2.calling_number = "789012".to_string();
        assert_eq!(pool.stats().current_index, 2);
    }

    #[test]
    fn test_event_pool_wrap_around() {
        let mut pool = EventPool::new(3);

        // Acquire 4 events - should wrap around
        for i in 0..4 {
            let event = pool.acquire();
            event.calling_number = i.to_string();
        }

        // After 4 acquisitions with capacity 3, index should be 1 (4 % 3)
        assert_eq!(pool.stats().current_index, 1);
    }

    #[test]
    fn test_event_pool_reset() {
        let mut pool = EventPool::new(10);

        let event = pool.acquire();
        event.calling_number = "999999".to_string();
        event.record_type = "mo_call".to_string();

        // Acquire again - should be reset
        let event2 = pool.acquire();
        event2.calling_number = "111111".to_string();

        // Next acquisition should give us a reset event
        let event3 = pool.acquire();
        // Values should be default ("" for strings)
        assert_eq!(event3.called_number, "");
    }
}
