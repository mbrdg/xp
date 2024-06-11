use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    hash::Hash,
};

pub trait Decomposable {
    type Decomposition;

    fn split(&self) -> Vec<Self::Decomposition>;
    fn join(&mut self, deltas: Vec<Self::Decomposition>);
    fn difference(&self, remote: &Self::Decomposition) -> Self::Decomposition;
}

pub trait Measurable {
    fn size_of(replica: &Self) -> usize;
    fn false_matches(&self, other: &Self) -> usize;
}

#[derive(PartialEq, Eq, Debug, Default)]
pub struct Elements<'a, T> {
    elems: Vec<&'a T>,
    idx: usize,
}

impl<'a, T> Iterator for Elements<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.elems.len() {
            return None;
        }

        self.idx += 1;
        Some(&self.elems[self.idx])
    }
}

#[derive(Clone, Debug, Default)]
pub struct GSet<T> {
    base: HashSet<T>,
}

impl<T> GSet<T> {
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            base: HashSet::new(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.base.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.base.len()
    }
}

impl<T> GSet<T>
where
    T: Eq + Hash,
{
    #[inline]
    pub fn contains(&self, value: &T) -> bool {
        self.base.contains(value)
    }
}

impl<T> GSet<T>
where
    T: Clone + Eq + Hash,
{
    #[inline]
    pub fn elements(&self) -> Elements<'_, T> {
        Elements {
            elems: self.base.iter().collect(),
            idx: 0,
        }
    }

    pub fn insert(&mut self, value: T) -> Self {
        if self.base.insert(value.clone()) {
            Self {
                base: HashSet::from([value]),
            }
        } else {
            Self {
                base: HashSet::new(),
            }
        }
    }
}

impl<T> Decomposable for GSet<T>
where
    T: Clone + Eq + Hash,
{
    type Decomposition = GSet<T>;

    fn split(&self) -> Vec<Self::Decomposition> {
        self.base
            .iter()
            .cloned()
            .map(|value| Self {
                base: HashSet::from([value]),
            })
            .collect()
    }

    fn join(&mut self, deltas: Vec<Self::Decomposition>) {
        deltas
            .into_iter()
            .for_each(|delta| self.base.extend(delta.base))
    }

    fn difference(&self, remote: &Self::Decomposition) -> Self::Decomposition {
        Self {
            base: self.base.difference(&remote.base).cloned().collect(),
        }
    }
}

impl Measurable for GSet<String> {
    fn query(&self) -> HashSet<String> {
        self.elements()
    }

    fn size_of(replica: &Self) -> usize {
        replica.base.iter().map(String::len).sum()
    }

    fn false_matches(&self, other: &Self) -> usize {
        self.base.symmetric_difference(&other.base).count()
    }
}

impl<T> PartialEq for GSet<T>
where
    T: Eq + Hash,
{
    fn eq(&self, other: &Self) -> bool {
        self.base == other.base
    }
}

impl<T> Eq for GSet<T> where T: Eq + Hash {}

#[cfg(test)]
mod gset {
    use super::*;

    #[test]
    fn test_split_and_join() {
        let mut splittable = GSet::new();

        splittable.insert(1);
        splittable.insert(2);
        splittable.insert(2);
        assert_eq!(splittable.len(), 2);

        let decompositions = splittable.split();
        assert_eq!(decompositions.len(), splittable.len());

        let mut joinable = GSet::new();

        joinable.join(decompositions);
        assert_eq!(joinable.len(), splittable.len());
        assert!(joinable.contains(&1));
        assert!(joinable.contains(&2));

        joinable.insert(3);
        assert_eq!(joinable.elements(), HashSet::from_iter(1..=3));
    }

    #[test]
    fn test_difference() {
        let local = GSet {
            base: HashSet::from_iter(0..=2),
        };
        let remote = GSet {
            base: HashSet::from_iter(2..=4),
        };

        let diff = local.difference(&remote);
        assert!(diff.contains(&0));
        assert!(diff.contains(&1));
        assert!(!diff.contains(&2));
        assert!(!diff.contains(&3));
        assert!(!diff.contains(&4));
    }

    #[test]
    fn test_difference_synced() {
        let local = GSet {
            base: HashSet::from_iter(0..3),
        };
        let remote = local.clone();

        assert_eq!(local.elements(), remote.elements());

        let diff = local.difference(&remote);
        assert!(diff.is_empty());
    }
}

#[derive(Clone, Debug, Default)]
pub struct GCounter<I> {
    base: HashMap<I, i64>,
}

impl<I> GCounter<I> {
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            base: HashMap::new(),
        }
    }

    #[inline]
    pub fn count(&self) -> i64 {
        self.base.values().sum()
    }
}

impl<I> GCounter<I>
where
    I: Clone + Eq + Hash,
{
    pub fn increment(&mut self, id: &I) -> Self {
        let increments = self
            .base
            .entry(id.clone())
            .and_modify(|inc| *inc += 1)
            .or_insert(1);

        Self {
            base: HashMap::from([(id.clone(), *increments)]),
        }
    }
}

impl<I> Decomposable for GCounter<I>
where
    I: Clone + Eq + Hash,
{
    type Decomposition = GCounter<I>;

    fn split(&self) -> Vec<Self::Decomposition> {
        self.base
            .clone()
            .into_iter()
            .map(|entry| Self {
                base: HashMap::from([entry]),
            })
            .collect()
    }

    fn join(&mut self, deltas: Vec<Self::Decomposition>) {
        deltas.into_iter().for_each(|delta| {
            delta.base.into_iter().for_each(|(id, v)| {
                self.base
                    .entry(id)
                    .and_modify(|inc| *inc = max(*inc, v))
                    .or_insert(v);
            })
        })
    }

    fn difference(&self, remote: &Self::Decomposition) -> Self::Decomposition {
        Self {
            base: HashMap::from_iter(
                self.base
                    .iter()
                    .filter(|(id, inc)| {
                        let entry = remote.base.get(id);
                        entry.is_none() || entry.is_some_and(|v| *inc > v)
                    })
                    .map(|(id, inc)| (id.clone(), *inc)),
            ),
        }
    }
}

#[cfg(test)]
mod gcounter {
    use super::*;

    #[test]
    fn test_increment() {
        let mut gcounter = GCounter::new();

        gcounter.increment(&1);
        gcounter.increment(&2);
        gcounter.increment(&3);
        gcounter.increment(&1);

        assert_eq!(gcounter.count(), 4);
    }

    #[test]
    fn test_split_and_join() {
        let splittable = GCounter {
            base: HashMap::from([(1, 1), (2, 2), (3, 1)]),
        };

        let decompositions = splittable.split();
        assert_eq!(decompositions.len(), 3);

        let mut joinable = GCounter::new();

        joinable.join(decompositions);
        assert_eq!(joinable.count(), 4);
        assert_eq!(splittable.base, joinable.base);
    }

    #[test]
    fn test_difference() {
        let local = GCounter {
            base: HashMap::from([(1, 1), (2, 3), (3, 2), (4, 1)]),
        };

        let mut remote = GCounter {
            base: HashMap::from([(1, 1), (2, 2), (3, 4), (5, 1)]),
        };

        let diff = local.difference(&remote);
        assert_eq!(diff.count(), 4);

        remote.join(vec![diff]);
        assert_eq!(remote.count(), 10);
        assert_eq!(
            remote.base,
            HashMap::from([(1, 1), (2, 3), (3, 4), (4, 1), (5, 1)])
        );
    }
}

#[derive(Clone, Debug, Default)]
pub struct AWSet<T> {
    inserted: HashMap<u64, T>,
    removed: HashSet<u64>,
}

impl<T> AWSet<T> {
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            inserted: HashMap::new(),
            removed: HashSet::new(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        !self.inserted.keys().any(|id| !self.removed.contains(id))
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.inserted
            .keys()
            .filter(|id| !self.removed.contains(id))
            .count()
    }
}

impl<T> AWSet<T>
where
    T: Eq + Hash,
{
    #[inline]
    pub fn contains(&self, value: &T) -> bool {
        self.inserted
            .iter()
            .any(|(id, v)| value == v && !self.removed.contains(id))
    }
}

impl<T> AWSet<T>
where
    T: Clone + Eq + Hash,
{
    #[inline]
    pub fn elements(&self) -> HashSet<T> {
        self.inserted
            .iter()
            .filter_map(|(id, value)| (!self.removed.contains(id)).then_some(value))
            .cloned()
            .collect()
    }

    pub fn insert(&mut self, value: T) -> Self {
        let id = max(self.inserted.keys().max(), self.removed.iter().max()).unwrap_or(&0) + 1;
        self.inserted.insert(id, value.clone());

        Self {
            inserted: HashMap::from([(id, value)]),
            removed: HashSet::new(),
        }
    }

    pub fn remove(&mut self, value: &T) -> Self {
        let id = self
            .inserted
            .iter()
            .find(|(id, v)| value == *v && !self.removed.contains(id))
            .map(|(id, _)| {
                self.removed.insert(*id);
                *id
            });

        if let Some(id) = id {
            Self {
                inserted: HashMap::new(),
                removed: HashSet::from([id]),
            }
        } else {
            Self {
                inserted: HashMap::new(),
                removed: HashSet::new(),
            }
        }
    }
}

impl<T> Decomposable for AWSet<T>
where
    T: Clone + Eq + Hash,
{
    type Decomposition = AWSet<T>;

    fn split(&self) -> Vec<Self::Decomposition> {
        let inserted = self.inserted.iter().map(|(id, v)| Self {
            inserted: HashMap::from([(*id, v.clone())]),
            removed: HashSet::new(),
        });

        let removed = self.removed.iter().cloned().map(|id| Self {
            inserted: HashMap::new(),
            removed: HashSet::from([id]),
        });

        inserted.chain(removed).collect()
    }

    fn join(&mut self, deltas: Vec<Self::Decomposition>) {
        deltas.into_iter().for_each(|delta| {
            self.inserted.extend(delta.inserted);
            self.removed.extend(delta.removed);
        })
    }

    fn difference(&self, remote: &Self::Decomposition) -> Self::Decomposition {
        Self {
            inserted: self
                .inserted
                .iter()
                .filter(|(id, _)| !remote.inserted.contains_key(id))
                .map(|(id, v)| (*id, v.clone()))
                .collect(),
            removed: self.removed.difference(&remote.removed).cloned().collect(),
        }
    }
}

impl Measurable for AWSet<String> {
    fn query(&self) -> HashSet<String> {
        self.elements()
    }

    fn size_of(replica: &Self) -> usize {
        replica
            .inserted
            .iter()
            .filter_map(|(id, v)| (!replica.removed.contains(id)).then_some(v))
            .map(String::len)
            .sum()
    }

    fn false_matches(&self, other: &Self) -> usize {
        self.inserted
            .iter()
            .filter_map(|(id, v)| (!self.removed.contains(id)).then_some(v))
            .filter(|v| !other.contains(v))
            .chain(
                other
                    .inserted
                    .iter()
                    .filter_map(|(id, v)| (!other.removed.contains(id)).then_some(v))
                    .filter(|v| !self.contains(v)),
            )
            .count()
    }
}

impl<T> PartialEq for AWSet<T>
where
    T: Eq + Hash,
{
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }

        self.inserted
            .iter()
            .filter_map(|(id, v)| (!self.removed.contains(id)).then_some(v))
            .all(|id| other.contains(id))
    }
}

impl<T> Eq for AWSet<T> where T: Eq + Hash {}

#[cfg(test)]
mod awset {
    use super::*;

    #[test]
    fn test_insert_and_remove() {
        let mut awset = AWSet::new();
        assert_eq!(awset.len(), 0);
        assert!(awset.is_empty());

        awset.insert(1);
        awset.insert(2);
        awset.insert(3);
        assert_eq!(awset.len(), 3);
        assert!(!awset.is_empty());

        awset.remove(&2);
        awset.remove(&2);
        awset.remove(&4);
        assert_eq!(awset.len(), 2);

        awset.insert(2);
        awset.insert(4);
        assert_eq!(awset.len(), 4);
        assert_eq!(awset.elements(), HashSet::from_iter(1..=4));
    }

    #[test]
    fn test_split_and_join() {
        let mut splittable = AWSet::new();

        splittable.insert(1);
        splittable.insert(2);
        splittable.insert(3);
        splittable.remove(&2);
        splittable.remove(&4);

        assert!(splittable.contains(&1));
        assert!(splittable.contains(&3));

        let decompositions = splittable.split();
        assert_eq!(decompositions.len(), 4);

        let mut joinable = AWSet::new();
        joinable.join(decompositions);

        assert_eq!(splittable, joinable);
    }

    #[test]
    fn test_difference() {
        let local = AWSet {
            inserted: HashMap::from([(1, 1), (2, 3), (3, 2), (4, 4), (5, 10)]),
            removed: HashSet::from([1, 3]),
        };

        let remote = AWSet {
            inserted: HashMap::from([(1, 1), (2, 3), (3, 2)]),
            removed: HashSet::from([1, 2]),
        };

        let diff = local.difference(&remote);
        assert_eq!(diff.inserted, HashMap::from([(4, 4), (5, 10)]));
        assert_eq!(diff.removed, HashSet::from([3]));
    }

    #[test]
    fn test_difference_synced() {
        let local = AWSet {
            inserted: HashMap::from([(1, 1), (2, 3), (3, 2), (4, 4), (5, 10)]),
            removed: HashSet::from([1, 3]),
        };

        let remote = AWSet {
            inserted: HashMap::from([(1, 1), (2, 3), (3, 2), (4, 4), (5, 10)]),
            removed: HashSet::from([1, 3]),
        };

        let diff = local.difference(&remote);
        assert!(diff.inserted.is_empty());
        assert!(diff.removed.is_empty());
    }
}
