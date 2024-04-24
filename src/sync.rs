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

pub trait Algorithm {
    type Replica;

    fn sync(&mut self) -> Metrics;
    fn size_of(replica: &Self::Replica) -> usize;
    fn is_synced(&self) -> bool;
}

pub struct Baseline {
    local: GSet<String>,
    remote: GSet<String>,
}

impl Baseline {
    #[inline]
    #[must_use]
    pub fn new(local: GSet<String>, remote: GSet<String>) -> Self {
        Self { local, remote }
    }
}

impl Algorithm for Baseline {
    type Replica = GSet<String>;

    fn sync(&mut self) -> Metrics {
        let mut metrics = Metrics::default();

        // 1. Ship the full local state and send them to the remote peer.
        let local_state = self.local.clone();
        metrics.bytes_exchanged += Baseline::size_of(&local_state);
        metrics.round_trips += 1;

        // 2. The remote peer computes the optimal delta from its current state.
        let remote_unseen = local_state.difference(&self.remote);
        let local_unseen = self.remote.difference(&local_state);

        self.remote.join(vec![remote_unseen]);

        metrics.bytes_exchanged += Baseline::size_of(&local_unseen);
        metrics.round_trips += 1;

        // 3. Merge the minimum delta from the remote peer.
        self.local.join(vec![local_unseen]);

        // 4. Sanity check, i.e., false matches must be 0.
        metrics.false_matches = self
            .local
            .elements()
            .symmetric_difference(self.remote.elements())
            .count();

        println!("=> Baseline\n\t=> {:?}", metrics);
        metrics
    }

    fn size_of(replica: &Self::Replica) -> usize {
        replica.elements().iter().map(String::len).sum()
    }

    fn is_synced(&self) -> bool {
        self.local == self.remote
    }
}

pub struct BucketDispatcher<const B: usize> {
    local: GSet<String>,
    remote: GSet<String>,
}

impl<const B: usize> BucketDispatcher<B> {
    #[inline]
    #[must_use]
    pub fn new(local: GSet<String>, remote: GSet<String>) -> Self {
        Self { local, remote }
    }
}

impl<const B: usize> Algorithm for BucketDispatcher<B> {
    type Replica = GSet<String>;

    fn sync(&mut self) -> Metrics {
        let mut metrics = Metrics::default();
        let s = RandomState::new();

        const BUCKET: Vec<(GSet<String>, u64)> = Vec::new();

        // 1. Split the local state into decompositions and assign them to a particular bucket.
        //    The policy is implementation defined, but here, each decomposition is assigned to a
        //    bucket based on the remainder from its hash value w.r.t. to the number of buckets.
        let mut local_buckets = [BUCKET; B];

        // 1.1. Compute the hash and assign to the corresponding bucket (locally).
        self.local.split().into_iter().for_each(|delta| {
            let hash = s.hash_one(
                delta
                    .elements()
                    .iter()
                    .next()
                    .expect("a decomposition should have a single item"),
            );

            let i = usize::try_from(hash).unwrap() % local_buckets.len();
            local_buckets[i].push((delta, hash));
        });

        // 1.2 Sort the bucket's elements based on its items hash values, i.e, Merkle trick.
        local_buckets
            .iter_mut()
            .for_each(|bucket| bucket.sort_unstable_by_key(|k| k.1));

        // 1.3. Compute the bucket hash value (locally).
        let local_hashes = local_buckets.iter().map(|bucket| {
            s.hash_one(
                bucket
                    .iter()
                    .fold(String::new(), |h, k| h + k.1.to_string().as_str()),
            )
        });

        // 1.4 Send the bucket hashes to the remote replica.
        metrics.bytes_exchanged += size_of::<u64>() * B;
        metrics.round_trips += 1;

        // 2. Split the remote state into decompositions and assign them to a particular bucket.
        //    Again, the policy is implementation defined, but it must be deterministic across
        //    different replicas.
        let mut remote_buckets = [BUCKET; B];

        // 2.1 Compute the hash and assign to the corresponding bucket (remotely).
        self.remote.split().into_iter().for_each(|delta| {
            let hash = s.hash_one(
                delta
                    .elements()
                    .iter()
                    .next()
                    .expect("a decomposition should have a single item"),
            );

            let i = usize::try_from(hash).unwrap() % remote_buckets.len();
            remote_buckets[i].push((delta, hash));
        });

        // 2.2 Sort the bucket's elements based on its items hash values, i.e, Merkle trick.
        remote_buckets
            .iter_mut()
            .for_each(|bucket| bucket.sort_unstable_by_key(|k| k.1));

        // 2.3. Aggregate the state from the non matching buckets. Or, if the hashes match, ship
        //   back an empty payload to the local replica.
        let non_matching_buckets: Vec<_> = remote_buckets
            .iter_mut()
            .zip(local_hashes)
            .map(|(bucket, local_hash)| {
                let remote_hash = s.hash_one(
                    bucket
                        .iter()
                        .fold(String::new(), |h, k| h + k.1.to_string().as_str()),
                );

                if remote_hash == local_hash {
                    return None;
                }

                let mut state = GSet::new();
                state.join(bucket.drain(..).map(|k| k.0).collect());

                Some(state)
            })
            .collect();

        // 2.4. Send the aggregated bucket state back to the local replica.
        metrics.bytes_exchanged += non_matching_buckets
            .iter()
            .flatten()
            .map(BucketDispatcher::<B>::size_of)
            .sum::<usize>();
        metrics.round_trips += 1;

        // 3. Compute the state that has been not yet seen by the local replica, and send back to
        //    remote peer the state that he has not seen yet. Only the difference, i.e., the
        //    optimal delta, computed from the join-decompositions, is sent back.
        let (local_unseen, remote_unseen) = local_buckets
            .into_iter()
            .map(|bucket| {
                let mut state = GSet::new();
                state.join(bucket.into_iter().map(|k| k.0).collect());

                state
            })
            .zip(non_matching_buckets)
            .flat_map(|buckets| match buckets.1 {
                Some(state) => Some((buckets.0, state)),
                None => None,
            })
            .fold((Vec::new(), Vec::new()), |mut unseen, buckets| {
                unseen.0.push(buckets.1.difference(&buckets.0));
                unseen.1.push(buckets.0.difference(&buckets.1));

                unseen
            });

        self.local.join(local_unseen);

        metrics.bytes_exchanged += remote_unseen
            .iter()
            .map(BucketDispatcher::<B>::size_of)
            .sum::<usize>();
        metrics.round_trips += 1;

        self.remote.join(remote_unseen);

        // 4. Sanity check, i.e., false matches must be 0.
        metrics.false_matches = self
            .local
            .elements()
            .symmetric_difference(self.remote.elements())
            .count();

        println!("=> BucketDispatcher w/ {} buckets\n\t=> {:?}", B, metrics);
        metrics
    }

    fn size_of(replica: &Self::Replica) -> usize {
        replica.elements().iter().map(String::len).sum()
    }

    fn is_synced(&self) -> bool {
        self.local == self.remote
    }
}
