use std::{
    collections::BTreeMap,
    hash::{BuildHasher, RandomState},
    mem,
};

use crate::{
    bloom::BloomFilter,
    crdt::{Decomposable, Extractable},
    tracker::Tracker,
};

pub mod baseline;
pub mod bloom;
pub mod bloombuckets;
pub mod buckets;

pub trait Protocol {
    type Tracker: Tracker;

    fn sync(&mut self, tracker: &mut Self::Tracker);
}

pub trait Dispatcher<T>
where
    T: Clone + Decomposable<Decomposition = T> + Extractable,
{
    fn dispatch<H: BuildHasher>(
        &self,
        replica: &T,
        len: usize,
        hasher: &H,
    ) -> Vec<BTreeMap<u64, T>> {
        let mut buckets = vec![BTreeMap::new(); len];

        replica.split().into_iter().for_each(|d| {
            let hash = hasher.hash_one(d.get());
            let idx = usize::try_from(hash).unwrap() % len;

            buckets[idx].insert(hash, d);
        });

        buckets
    }

    fn hashes<H: BuildHasher>(buckets: &[BTreeMap<u64, T>], hasher: &H) -> Vec<u64> {
        buckets
            .iter()
            .map(|b| hasher.hash_one(b.keys().fold(String::new(), |acc, h| format!("{acc}{h}"))))
            .collect()
    }
}

pub trait BloomBased<T>
where
    T: Extractable,
{
    fn filter_from(&self, decompositions: &[T], fpr: f64) -> BloomFilter<<T as Extractable>::Item> {
        let mut filter = BloomFilter::new(decompositions.len(), fpr);
        decompositions.iter().for_each(|d| filter.insert(&d.get()));

        filter
    }

    fn partition(
        &self,
        filter: &BloomFilter<<T as Extractable>::Item>,
        decompositions: Vec<T>,
    ) -> (Vec<T>, Vec<T>) {
        decompositions
            .into_iter()
            .partition(|d| filter.contains(&d.get()))
    }

    fn size_of(filter: &BloomFilter<<T as Extractable>::Item>) -> usize {
        filter.bitslice().chunks(8).count()
            + mem::size_of::<RandomState>() * 2
            + mem::size_of::<u64>()
    }
}
