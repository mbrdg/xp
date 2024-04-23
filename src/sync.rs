use std::{
    hash::{BuildHasher, RandomState},
    mem::size_of,
};

use crate::crdt::{Decomposable, GSet};

#[derive(Debug, Default)]
pub struct Metrics {
    bytes_exchanged: usize,
    round_trips: u8,
    false_matches: usize,
}

pub trait Algorithm<R> {
    fn sync(&mut self) -> Metrics;
    fn size_of(replica: &R) -> usize;
    fn is_synced(&self) -> bool;
}

pub struct Baseline {
    local: GSet<String>,
    remote: GSet<String>,
}

impl Baseline {
    #[allow(dead_code)]
    #[inline]
    #[must_use]
    pub fn new(local: GSet<String>, remote: GSet<String>) -> Self {
        Self { local, remote }
    }
}

impl Algorithm<GSet<String>> for Baseline {
    fn sync(&mut self) -> Metrics {
        let mut metrics = Metrics::default();

        // 1. Ship the full local state and send them to the remote peer
        let local_state = self.local.clone();
        metrics.bytes_exchanged += Baseline::size_of(&local_state);
        metrics.round_trips += 1;

        // 2. The remote peer computes the optimal delta from its current state
        let remote_unseen = local_state.difference(&self.remote);
        let local_unseen = self.remote.difference(&local_state);

        self.remote.join(vec![remote_unseen]);

        metrics.bytes_exchanged += Baseline::size_of(&local_unseen);
        metrics.round_trips += 1;

        // 3. Merge the minimum delta from the remote peer
        self.local.join(vec![local_unseen]);

        // 4. sanity check, i.e., false matches must be 0
        metrics.false_matches = self
            .local
            .elements()
            .symmetric_difference(self.remote.elements())
            .count();

        println!("=> Baseline\n\t=> {:?}", metrics);
        metrics
    }

    fn size_of(replica: &GSet<String>) -> usize {
        replica.elements().iter().map(String::len).sum()
    }

    fn is_synced(&self) -> bool {
        self.local.elements() == self.remote.elements()
    }
}

pub struct BucketDispatcher<const B: usize> {
    local: GSet<String>,
    remote: GSet<String>,
}

impl<const B: usize> BucketDispatcher<B> {
    #[allow(dead_code)]
    #[inline]
    #[must_use]
    pub fn new(local: GSet<String>, remote: GSet<String>) -> Self {
        Self { local, remote }
    }
}

impl<const B: usize> Algorithm<GSet<String>> for BucketDispatcher<B> {
    fn sync(&mut self) -> Metrics {
        let mut metrics = Metrics::default();
        let s = RandomState::new();

        const BUCKET: Vec<(GSet<String>, u64)> = Vec::new();

        let mut local_buckets = [BUCKET; B];
        self.local.split().into_iter().for_each(|decomposition| {
            let hash = s.hash_one(
                decomposition
                    .elements()
                    .iter()
                    .next()
                    .expect("a decomposition should have a single item"),
            );

            let i = usize::try_from(hash).unwrap() % local_buckets.len();
            local_buckets[i].push((decomposition, hash));
        });

        local_buckets
            .iter_mut()
            .for_each(|bucket| bucket.sort_unstable_by_key(|k| k.1));

        let local_hashes = local_buckets.iter().map(|bucket| {
            s.hash_one(
                bucket
                    .iter()
                    .fold(String::new(), |h, k| h + k.1.to_string().as_str()),
            )
        });

        metrics.bytes_exchanged += size_of::<u64>() * B;
        metrics.round_trips += 1;

        let mut remote_buckets = [BUCKET; B];
        self.remote.split().into_iter().for_each(|decomposition| {
            let hash = s.hash_one(
                decomposition
                    .elements()
                    .iter()
                    .next()
                    .expect("a decomposition should have a single item"),
            );

            let i = usize::try_from(hash).unwrap() % remote_buckets.len();
            remote_buckets[i].push((decomposition, hash));
        });

        remote_buckets
            .iter_mut()
            .for_each(|bucket| bucket.sort_unstable_by_key(|k| k.1));

        let non_matching_buckets: Vec<GSet<String>> = remote_buckets
            .iter_mut()
            .zip(local_hashes)
            .map(|(bucket, local_hash)| {
                let remote_hash = s.hash_one(
                    bucket
                        .iter()
                        .fold(String::new(), |h, k| h + k.1.to_string().as_str()),
                );

                let mut state = GSet::new();
                if remote_hash != local_hash {
                    state.join(bucket.drain(..).map(|k| k.0).collect());
                }

                state
            })
            .collect();

        metrics.bytes_exchanged += non_matching_buckets
            .iter()
            .map(BucketDispatcher::<B>::size_of)
            .sum::<usize>();
        metrics.round_trips += 1;

        let (local_unseen, remote_unseen) = local_buckets
            .into_iter()
            .map(|bucket| {
                let mut gset = GSet::new();
                gset.join(bucket.into_iter().map(|k| k.0).collect());

                gset
            })
            .zip(non_matching_buckets)
            .filter(|buckets| !buckets.1.is_empty())
            .fold(
                (Vec::with_capacity(B), Vec::with_capacity(B)),
                |mut unseen, (local_bucket, non_matching_bucket)| {
                    unseen.0.push(non_matching_bucket.difference(&local_bucket));
                    unseen.1.push(local_bucket.difference(&non_matching_bucket));
                    unseen
                },
            );

        self.local.join(local_unseen);

        metrics.bytes_exchanged += remote_unseen
            .iter()
            .map(BucketDispatcher::<B>::size_of)
            .sum::<usize>();
        metrics.round_trips += 1;

        self.remote.join(remote_unseen);

        metrics.false_matches = self
            .local
            .elements()
            .symmetric_difference(self.remote.elements())
            .count();

        println!("=> BucketDispatcher w/ {} buckets\n\t=> {:?}", B, metrics);
        metrics
    }

    fn size_of(replica: &GSet<String>) -> usize {
        replica.elements().iter().map(String::len).sum()
    }

    fn is_synced(&self) -> bool {
        self.local.elements() == self.remote.elements()
    }
}
