use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use crate::sql_str::SqlStr;

/// Queries to run the next time a connection is locked.
///
/// Drivers use this to queue a `ROLLBACK` when a transaction is dropped while the connection
/// is locked elsewhere, which happens if middleware keeps a response stream alive after its
/// request completed.
#[derive(Debug, Default)]
pub struct PendingQueries {
    // Lets `take()` skip the lock in the common case where nothing was queued.
    //
    // Queries are only pushed by the owner of the connection handle, so a push always
    // happens-before the next time the owner locks the connection.
    queued: AtomicBool,
    queries: Mutex<Vec<SqlStr>>,
}

impl PendingQueries {
    /// Queue a query.
    pub fn push(&self, query: SqlStr) {
        let mut queries = self.queries.lock().unwrap_or_else(|e| e.into_inner());
        queries.push(query);
        self.queued.store(true, Ordering::Release);
    }

    /// Remove and return all queued queries, in the order they were queued.
    pub fn take(&self) -> Vec<SqlStr> {
        if !self.queued.load(Ordering::Acquire) {
            return Vec::new();
        }

        let mut queries = self.queries.lock().unwrap_or_else(|e| e.into_inner());
        self.queued.store(false, Ordering::Relaxed);
        std::mem::take(&mut *queries)
    }
}
