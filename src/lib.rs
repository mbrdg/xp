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
    pub fn new() -> Self {
        Self {
            base: HashSet::new(),
        }
    }

    pub fn contains(&self, value: &T) -> bool {
        self.base.contains(value)
    }

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

    pub fn is_empty(&self) -> bool {
        self.base.is_empty()
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_and_join() {
        let mut splittable = GSet::new();

        splittable.insert(1);
        splittable.insert(2);
        splittable.insert(2);

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
        let mut local = GSet::new();
        let mut remote = GSet::new();

        local.insert(0);
        local.insert(1);
        local.insert(2);

        remote.insert(2);
        remote.insert(3);
        remote.insert(4);

        let diff = local.difference(&remote);

        assert!(diff.contains(&0));
        assert!(diff.contains(&1));
        assert!(!diff.contains(&2));
        assert!(!diff.contains(&3));
        assert!(!diff.contains(&4));
    }
}
