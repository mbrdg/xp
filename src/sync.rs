use std::{
    collections::BTreeMap,
    hash::{BuildHasher, RandomState},
    mem,
};

use crate::{
    bloom::BloomFilter,
    crdt::{Decompose, Extract},
    tracker::Telemetry,
};

pub mod baseline;
pub mod bloom;
pub mod bloombuckets;
pub mod buckets;

pub trait Algorithm<T> {
    type Tracker: Telemetry;

    fn sync(&self, local: &mut T, remote: &mut T, tracker: &mut Self::Tracker);
}

pub trait Dispatcher<T>
where
    T: Clone + Decompose<Decomposition = T> + Extract,
{
    fn dispatch<H: BuildHasher>(
        &self,
        replica: &T,
        len: usize,
        hasher: &H,
    ) -> Vec<BTreeMap<u64, T>> {
        let mut buckets = vec![BTreeMap::new(); len];

        replica.split().into_iter().for_each(|d| {
            let hash = hasher.hash_one(d.extract());
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

pub trait BuildFilter<T>
where
    T: Extract,
{
    fn filter_from(&self, decompositions: &[T], fpr: f64) -> BloomFilter<<T as Extract>::Item> {
        let mut filter = BloomFilter::new(decompositions.len(), fpr);
        decompositions
            .iter()
            .for_each(|d| filter.insert(&d.extract()));

        filter
    }

    fn partition(
        &self,
        filter: &BloomFilter<<T as Extract>::Item>,
        decompositions: Vec<T>,
    ) -> (Vec<T>, Vec<T>) {
        decompositions
            .into_iter()
            .partition(|d| filter.contains(&d.extract()))
    }

    fn size_of(filter: &BloomFilter<<T as Extract>::Item>) -> usize {
        filter.bitslice().chunks(8).count()
            + mem::size_of::<RandomState>() * 2
            + mem::size_of::<u64>()
    }
}
