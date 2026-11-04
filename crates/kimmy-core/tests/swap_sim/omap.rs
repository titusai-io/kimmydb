//! An insertion-ordered map and set. The Python model iterates dicts in insertion order and several of
//! its side effects (the order in which loss entries are minted, the order `sweep` walks the physicals)
//! depend on it, so the port needs the same order, and the harness takes no new dependency for it.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

#[derive(Clone, Debug)]
pub struct OMap<K, V> {
    items: Vec<(K, V)>,
    index: HashMap<K, usize>,
}

impl<K, V> Default for OMap<K, V> {
    fn default() -> Self {
        Self { items: Vec::new(), index: HashMap::new() }
    }
}

impl<K: Clone + Eq + Hash, V> OMap<K, V> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn len(&self) -> usize {
        self.items.len()
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
    pub fn contains_key<Q: ?Sized + Hash + Eq>(&self, k: &Q) -> bool
    where
        K: Borrow<Q>,
    {
        self.index.contains_key(k)
    }
    pub fn get<Q: ?Sized + Hash + Eq>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
    {
        self.index.get(k).map(|&i| &self.items[i].1)
    }
    pub fn get_mut<Q: ?Sized + Hash + Eq>(&mut self, k: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
    {
        match self.index.get(k) {
            Some(&i) => Some(&mut self.items[i].1),
            None => None,
        }
    }
    /// Insert or replace; a replaced key keeps its position (Python dict semantics).
    pub fn insert(&mut self, k: K, v: V) -> Option<V> {
        if let Some(&i) = self.index.get(&k) {
            return Some(std::mem::replace(&mut self.items[i].1, v));
        }
        self.index.insert(k.clone(), self.items.len());
        self.items.push((k, v));
        None
    }
    pub fn remove<Q: ?Sized + Hash + Eq>(&mut self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
    {
        let i = self.index.remove(k)?;
        let (_, v) = self.items.remove(i);
        for (kk, _) in &self.items[i..] {
            if let Some(slot) = HashMap::get_mut::<K>(&mut self.index, kk) {
                *slot -= 1;
            }
        }
        Some(v)
    }
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.items.iter().map(|(k, v)| (k, v))
    }
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.items.iter().map(|(k, _)| k)
    }
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.items.iter().map(|(_, v)| v)
    }
    pub fn clear(&mut self) {
        self.items.clear();
        self.index.clear();
    }
    /// Keep the entries for which `f` is true, in their order.
    pub fn retain(&mut self, mut f: impl FnMut(&K, &V) -> bool) {
        self.items.retain(|(k, v)| f(k, v));
        self.index.clear();
        for (i, (k, _)) in self.items.iter().enumerate() {
            self.index.insert(k.clone(), i);
        }
    }
}

/// An insertion-ordered set.
#[derive(Clone, Debug)]
pub struct OSet<K>(OMap<K, ()>);

impl<K> Default for OSet<K> {
    fn default() -> Self {
        Self(OMap::default())
    }
}

impl<K: Clone + Eq + Hash> OSet<K> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn contains(&self, k: &K) -> bool {
        self.0.contains_key(k)
    }
    pub fn insert(&mut self, k: K) -> bool {
        self.0.insert(k, ()).is_none()
    }
    pub fn remove(&mut self, k: &K) -> bool {
        self.0.remove(k).is_some()
    }
    pub fn iter(&self) -> impl Iterator<Item = &K> {
        self.0.keys()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omap_keeps_insertion_order_and_replaced_keys_keep_their_place() {
        let mut m: OMap<&str, i32> = OMap::new();
        m.insert("b", 1);
        m.insert("a", 2);
        m.insert("c", 3);
        m.insert("a", 9);
        assert_eq!(m.keys().copied().collect::<Vec<_>>(), ["b", "a", "c"]);
        assert_eq!(m.get(&"a"), Some(&9));
        assert_eq!(m.remove(&"b"), Some(1));
        assert_eq!(m.keys().copied().collect::<Vec<_>>(), ["a", "c"]);
        assert_eq!(m.get(&"c"), Some(&3));
        m.insert("b", 4);
        assert_eq!(m.keys().copied().collect::<Vec<_>>(), ["a", "c", "b"]);
        m.retain(|k, _| *k != "a");
        assert_eq!(m.keys().copied().collect::<Vec<_>>(), ["c", "b"]);
        assert_eq!(m.get(&"b"), Some(&4));
    }

    #[test]
    fn oset_basics() {
        let mut s: OSet<u32> = OSet::new();
        assert!(s.insert(3));
        assert!(!s.insert(3));
        assert!(s.insert(1));
        assert_eq!(s.iter().copied().collect::<Vec<_>>(), [3, 1]);
        assert!(s.remove(&3));
        assert!(!s.contains(&3));
    }
}
