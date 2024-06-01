use std::{cmp::min, hash::BuildHasher, mem};

use crate::{bloom::BloomFilter, crdt::Decomposable, tracker::Tracker};

pub mod baseline;
pub mod bloom;
pub mod bloombuckets;
pub mod buckets;

trait ReplicaSize {
    type Replica: Decomposable;

    fn size_of(replica: &Self::Replica) -> usize;
}

pub trait Protocol {
    type Replica: Decomposable;
    type Builder: BuildProtocol;
    type Tracker: Tracker;

    fn builder(local: Self::Replica, remote: Self::Replica) -> Self::Builder;
    fn sync(&mut self, tracker: &mut Self::Tracker);
}

pub trait BuildProtocol {
    type Protocol: Protocol;

    fn build(self) -> Self::Protocol;
}

trait BuildBloomFilters {
    type Decomposition: Decomposable;
    type Item;

    fn filter(decompositions: &[Self::Decomposition], fpr: f64) -> BloomFilter<Self::Item>;

    fn size_of(filter: &BloomFilter<Self::Item>) -> usize {
        let bitslice = filter.bitslice();
        let hashers = filter.hashers();

        bitslice.len() / 8 + min(bitslice.len() % 8, 1) + mem::size_of_val(hashers)
    }
}

trait BuildBuckets {
    type Replica: Decomposable;
    type Hasher: BuildHasher;
    type Bucket;

    fn buckets(
        replica: &Self::Replica,
        hasher: &Self::Hasher,
        num_buckets: usize,
    ) -> Vec<Self::Bucket>;

    fn hashes(buckets: &[Self::Bucket], hasher: &Self::Hasher) -> Vec<u64>;
}
