mod pending_queries;
mod statement_cache;

pub use pending_queries::PendingQueries;
pub use statement_cache::StatementCache;

use std::sync::atomic::{AtomicUsize, Ordering};

/// Decrement `counter` unless it's zero, returning its previous value.
///
/// (`AtomicUsize::fetch_update()` does this, but it's deprecated in newer versions of Rust.)
pub fn decrement_nonzero(counter: &AtomicUsize) -> Option<usize> {
    let mut current = counter.load(Ordering::Acquire);

    loop {
        let new = current.checked_sub(1)?;

        match counter.compare_exchange_weak(current, new, Ordering::AcqRel, Ordering::Acquire) {
            Ok(previous) => return Some(previous),
            Err(actual) => current = actual,
        }
    }
}
use std::fmt::{Debug, Formatter};
use std::ops::{Deref, DerefMut};

/// A wrapper for `Fn`s that provides a debug impl that just says "Function"
pub struct DebugFn<F: ?Sized>(pub F);

impl<F: ?Sized> Deref for DebugFn<F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<F: ?Sized> DerefMut for DebugFn<F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<F: ?Sized> Debug for DebugFn<F> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Function").finish()
    }
}
