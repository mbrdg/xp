use std::{
    collections::BTreeMap,
    hash::{BuildHasher, Hash, RandomState},
    marker::PhantomData,
    mem,
};

use crate::{
    bloom::BloomFilter,
    crdt::{AWSet, Decomposable, GSet},
    tracker::Tracker,
};

pub mod baseline;
pub mod bloom;
pub mod bloombuckets;
pub mod buckets;

pub trait Protocol {
    type Replica: Decomposable;
    type Tracker: Tracker;

    fn size_of(replica: &Self::Replica) -> usize;
    fn sync(&mut self, tracker: &mut Self::Tracker);
}

type Bucket<T> = BTreeMap<u64, T>;

#[derive(Clone, Debug, Default)]
pub struct BucketDispatcher<T> {
    num_buckets: usize,
    hasher: RandomState,
    _marker: PhantomData<T>,
}

impl<T> BucketDispatcher<T>
where
    T: Decomposable,
{
    pub fn new(num_buckets: usize) -> Self {
        assert_ne!(num_buckets, 0, "at least one bucket should exist");

        Self {
            num_buckets,
            hasher: RandomState::new(),
            _marker: PhantomData,
        }
    }

    fn hashes(&self, buckets: &[Bucket<T>]) -> Vec<u64> {
        buckets
            .iter()
            .map(|bucket| {
                let id = bucket
                    .keys()
                    .fold(String::new(), |acc, h| format!("{acc}{h}"));

                self.hasher.hash_one(id)
            })
            .collect()
    }
}

impl<T> BucketDispatcher<GSet<T>>
where
    T: Clone + Eq + Hash,
{
    fn dispatch(&self, replica: &GSet<T>) -> Vec<Bucket<GSet<T>>> {
        let mut buckets = vec![Bucket::new(); self.num_buckets];

        replica.split().into_iter().for_each(|delta| {
            let item = delta
                .elements()
                .iter()
                .next()
                .expect("a decomposition should have a single item");

            let hash = self.hasher.hash_one(item);
            let idx = usize::try_from(hash).unwrap() % buckets.len();

            buckets[idx].insert(hash, delta);
        });

        buckets
    }
}

impl<T> BucketDispatcher<AWSet<T>>
where
    T: Clone + Eq + Hash,
{
    fn dispatch(&self, replica: &AWSet<T>) -> Vec<Bucket<AWSet<T>>> {
        let mut buckets = vec![Bucket::new(); self.num_buckets];

        replica.split().into_iter().for_each(|delta| {
            let item = delta
                .elements()
                .into_iter()
                .next()
                .expect("a decomposition should have a single item");

            let hash = self.hasher.hash_one(item);
            let idx = usize::try_from(hash).unwrap() % buckets.len();

            buckets[idx].insert(hash, delta);
        });

        buckets
    }
}

#[derive(Debug)]
pub struct Bloomer<T> {
    fpr: f64,
    _marker: PhantomData<T>,
}

impl<T> Bloomer<T> {
    #[inline]
    #[must_use]
    pub fn new(fpr: f64) -> Self {
        assert!(
            fpr > 0.0 && (..1.0).contains(&fpr),
            "false positive rate should be a ratio greater than 0.0"
        );

        Self {
            fpr,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub const fn fpr(&self) -> f64 {
        self.fpr
    }

    #[inline]
    pub fn size_of(filter: &BloomFilter<T>) -> usize {
        filter.bitslice().chunks(8).count()
            + mem::size_of::<RandomState>() * 2
            + mem::size_of::<u64>()
    }
}

impl<T> Bloomer<GSet<T>>
where
    T: Hash,
{
    fn filter_from(&self, decompositions: &[GSet<T>]) -> BloomFilter<T> {
        let mut filter = BloomFilter::new(decompositions.len(), self.fpr);

        decompositions.iter().for_each(|delta| {
            let item = delta
                .elements()
                .iter()
                .next()
                .expect("a decomposition should have a single element");
            filter.insert(item);
        });

        filter
    }

    fn partition(
        &self,
        filter: &BloomFilter<T>,
        decompositions: Vec<GSet<T>>,
    ) -> (Vec<GSet<T>>, Vec<GSet<T>>) {
        decompositions.into_iter().partition(|delta| {
            let item = delta
                .elements()
                .iter()
                .next()
                .expect("a decomposition should have a single item");
            filter.contains(item)
        })
    }
}

impl<T> Bloomer<AWSet<T>>
where
    T: Clone + Eq + Hash,
{
    fn filter_from(&self, decompositions: &[AWSet<T>]) -> BloomFilter<T> {
        let mut filter = BloomFilter::new(decompositions.len(), self.fpr);

        decompositions.iter().for_each(|delta| {
            let item = delta
                .elements()
                .into_iter()
                .next()
                .expect("a decomposition should have a single element");
            filter.insert(&item);
        });

        filter
    }

    fn partition(
        &self,
        filter: &BloomFilter<T>,
        decompositions: Vec<AWSet<T>>,
    ) -> (Vec<AWSet<T>>, Vec<AWSet<T>>) {
        decompositions.into_iter().partition(|delta| {
            let item = delta
                .elements()
                .into_iter()
                .next()
                .expect("a decomposition should have a single item");
            filter.contains(&item)
        })
    }
}
