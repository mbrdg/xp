use std::{collections::HashSet, hash::Hash};

pub trait Decomposable {
    type Output;

    fn split(&self) -> Vec<Self::Output>;
    fn join(&mut self, deltas: Vec<Self::Output>);
    fn difference(&self, remote: &Self::Output) -> Self::Output;
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
    type Output = GSet<T>;

    fn split(&self) -> Vec<Self::Output> {
        self.base
            .iter()
            .cloned()
            .map(|value| Self {
                base: HashSet::from([value]),
            })
            .collect()
    }

    fn join(&mut self, deltas: Vec<Self::Output>) {
        deltas
            .into_iter()
            .for_each(|delta| self.base.extend(delta.base))
    }

    fn difference(&self, remote: &Self::Output) -> Self::Output {
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
mod tests {
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
