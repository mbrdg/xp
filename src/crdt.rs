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

#[derive(Clone, Debug, Default)]
pub struct GSet<T> {
    base: HashSet<T>,
}

impl<T> GSet<T>
where
    T: Eq + Hash + Clone,
{
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            base: HashSet::new(),
        }
    }

    #[inline]
    pub fn contains(&self, value: &T) -> bool {
        self.base.contains(value)
    }

    #[inline]
    pub fn elements(&self) -> &HashSet<T> {
        &self.base
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

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.base.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.base.len()
    }
}

impl<T> Decomposable for GSet<T>
where
    T: Eq + Hash + Clone,
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
    base: HashMap<I, i32>,
}

impl<I> GCounter<I>
where
    I: Clone + Eq + Hash,
{
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            base: HashMap::new(),
        }
    }

    #[inline]
    pub fn count(&self) -> i32 {
        self.base.values().sum()
    }

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
