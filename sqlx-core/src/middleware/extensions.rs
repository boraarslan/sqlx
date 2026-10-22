use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::hash::{BuildHasherDefault, Hasher};

type AnyMap = HashMap<TypeId, Box<dyn AnyClone + Send + Sync>, BuildHasherDefault<IdHasher>>;

/// A type map of arbitrary values attached to a [`Request`][super::Request].
///
/// Middleware can use this to pass data to other middleware further down the stack,
/// for example a span or a label for metrics.
///
/// This is modeled after `http::Extensions`; values are keyed by their type.
/// The map is only allocated once a value is inserted.
#[derive(Clone, Default)]
pub struct Extensions {
    map: Option<Box<AnyMap>>,
}

impl Extensions {
    /// Create an empty `Extensions` map. Does not allocate.
    #[inline]
    pub fn new() -> Self {
        Self { map: None }
    }

    /// Insert a value into the map, returning the previous value of the same type, if any.
    pub fn insert<T: Clone + Send + Sync + 'static>(&mut self, value: T) -> Option<T> {
        self.map
            .get_or_insert_with(Default::default)
            .insert(TypeId::of::<T>(), Box::new(value))
            .and_then(downcast_owned)
    }

    /// Get a reference to a value of type `T`, if one was inserted.
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T> {
        // Note: `Box<dyn AnyClone>` itself implements `AnyClone`,
        // so we must be careful to dereference it before calling its methods.
        let value: &(dyn AnyClone + Send + Sync) = &**self.map.as_ref()?.get(&TypeId::of::<T>())?;
        value.as_any().downcast_ref()
    }

    /// Get a mutable reference to a value of type `T`, if one was inserted.
    pub fn get_mut<T: Send + Sync + 'static>(&mut self) -> Option<&mut T> {
        let value: &mut (dyn AnyClone + Send + Sync) =
            &mut **self.map.as_mut()?.get_mut(&TypeId::of::<T>())?;
        value.as_any_mut().downcast_mut()
    }

    /// Get a mutable reference to a value of type `T`, inserting the result of `f` if it is
    /// not present.
    pub fn get_or_insert_with<T, F>(&mut self, f: F) -> &mut T
    where
        T: Clone + Send + Sync + 'static,
        F: FnOnce() -> T,
    {
        let value: &mut (dyn AnyClone + Send + Sync) = &mut **self
            .map
            .get_or_insert_with(Default::default)
            .entry(TypeId::of::<T>())
            .or_insert_with(|| Box::new(f()));

        value
            .as_any_mut()
            .downcast_mut()
            .expect("BUG: extension stored under the wrong `TypeId`")
    }

    /// Remove a value of type `T` from the map, returning it if it was present.
    pub fn remove<T: Send + Sync + 'static>(&mut self) -> Option<T> {
        self.map
            .as_mut()?
            .remove(&TypeId::of::<T>())
            .and_then(downcast_owned)
    }

    /// Remove all values from the map.
    pub fn clear(&mut self) {
        if let Some(map) = &mut self.map {
            map.clear();
        }
    }

    /// Returns `true` if no values are in the map.
    pub fn is_empty(&self) -> bool {
        self.map.as_ref().is_none_or(|map| map.is_empty())
    }

    /// Returns the number of values in the map.
    pub fn len(&self) -> usize {
        self.map.as_ref().map_or(0, |map| map.len())
    }

    /// Move all values from `other` into `self`, overwriting values of the same type.
    pub fn extend(&mut self, other: Self) {
        let Some(other) = other.map else {
            return;
        };

        match &mut self.map {
            Some(map) => map.extend(*other),
            None => self.map = Some(other),
        }
    }
}

impl Debug for Extensions {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Extensions")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

fn downcast_owned<T: 'static>(value: Box<dyn AnyClone + Send + Sync>) -> Option<T> {
    AnyClone::into_any(value)
        .downcast()
        .ok()
        .map(|boxed| *boxed)
}

trait AnyClone: Any {
    fn clone_box(&self) -> Box<dyn AnyClone + Send + Sync>;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

impl<T: Clone + Send + Sync + 'static> AnyClone for T {
    fn clone_box(&self) -> Box<dyn AnyClone + Send + Sync> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl Clone for Box<dyn AnyClone + Send + Sync> {
    fn clone(&self) -> Self {
        (**self).clone_box()
    }
}

// `TypeId` is already a hash, so there's no point in hashing it again.
#[derive(Default)]
struct IdHasher(u64);

impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        // `TypeId` currently only calls `write_u64()`, but this keeps us correct regardless.
        for &byte in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(byte);
        }
    }

    fn write_u64(&mut self, id: u64) {
        self.0 = id;
    }
}

#[cfg(test)]
mod tests {
    use super::Extensions;

    #[derive(Clone, Debug, PartialEq)]
    struct Label(&'static str);

    #[test]
    fn insert_get_remove() {
        let mut ext = Extensions::new();
        assert!(ext.is_empty());
        assert_eq!(ext.insert(Label("a")), None);
        assert_eq!(ext.insert(5i32), None);
        assert_eq!(ext.insert(Label("b")), Some(Label("a")));
        assert_eq!(ext.len(), 2);
        assert_eq!(ext.get::<Label>(), Some(&Label("b")));
        *ext.get_mut::<i32>().unwrap() += 1;
        assert_eq!(ext.get::<i32>(), Some(&6));

        let cloned = ext.clone();
        assert_eq!(ext.remove::<Label>(), Some(Label("b")));
        assert_eq!(ext.get::<Label>(), None);
        assert_eq!(cloned.get::<Label>(), Some(&Label("b")));

        let mut other = Extensions::new();
        other.extend(cloned);
        assert_eq!(other.get::<i32>(), Some(&6));
        assert_eq!(*other.get_or_insert_with(|| 0u8), 0);
    }
}
