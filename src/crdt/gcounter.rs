use std::{cmp::max, collections::HashMap, hash::Hash};

use super::Decomposable;

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
mod tests {
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
