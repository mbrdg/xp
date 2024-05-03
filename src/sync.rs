use std::{
    cmp::min,
    hash::{BuildHasher, RandomState},
};

use crate::{
    bloom::BloomFilter,
    crdt::{Decomposable, GSet},
    tracker::{DefaultTracker, NetworkHop, Tracker},
};

pub trait Algorithm {
    type Replica;
    type Tracker;

    fn sync(&mut self) -> Self::Tracker;
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
    type Tracker = DefaultTracker;

    fn sync(&mut self) -> Self::Tracker {
        let mut tracker = Self::Tracker::new();

        // 1. Ship the full local state and send it the remote replica.
        let local_state = self.local.clone();

        tracker.register(NetworkHop::LocalToRemote(Baseline::size_of(&local_state)));

        // 2.1. Compute the optimal delta based on the remote replica state.
        let remote_unseen = local_state.difference(&self.remote);
        let local_unseen = self.remote.difference(&local_state);

        // 2.2. Join the decompositions that are unknown to the remote replica.
        self.remote.join(vec![remote_unseen]);

        // 2.3. Send back to the local replica the decompositions unknown to the local replica.
        tracker.register(NetworkHop::RemoteToLocal(Baseline::size_of(&local_unseen)));

        // 3. Merge the minimum delta received from the remote replica.
        self.local.join(vec![local_unseen]);

        // 4. Sanity check.
        // NOTE: This algorithm guarantees that replicas sync given that no operations occur.
        tracker.finish(
            self.local
                .elements()
                .symmetric_difference(self.remote.elements())
                .count(),
        );

        tracker
    }

    fn size_of(replica: &Self::Replica) -> usize {
        replica.elements().iter().map(String::len).sum()
    }

    fn is_synced(&self) -> bool {
        self.local == self.remote
    }
}

pub struct Probabilistic {
    local: GSet<String>,
    remote: GSet<String>,
}

impl Probabilistic {
    #[inline]
    #[must_use]
    pub fn new(local: GSet<String>, remote: GSet<String>) -> Self {
        Self { local, remote }
    }
}

impl Algorithm for Probabilistic {
    type Replica = GSet<String>;
    type Tracker = DefaultTracker;

    fn sync(&mut self) -> Self::Tracker {
        let mut tracker = Self::Tracker::new();
        const FPR: f64 = 0.0001; // 0.01 %

        // 1.1. Split the local state and insert each decomposition into a Bloom Filter.
        let local_split = self.local.split();
        let mut local_filter = BloomFilter::with_capacity_and_fpr(local_split.len(), FPR);

        local_split.iter().for_each(|delta| {
            let item = delta
                .elements()
                .iter()
                .next()
                .expect("a decomposition should have a single element");
            local_filter.insert(item);
        });

        // 1.2. Ship the Bloom filter to the remote replica.
        let local_filter_len = local_filter.as_bitslice().len();
        tracker.register(NetworkHop::LocalToRemote(
            (local_filter_len / 8 + min(1, local_filter_len % 8))
                + 2 * std::mem::size_of::<RandomState>(),
        ));

        // 2.1. At the remote replica, split the state into join decompositions. Then split the
        //   decompositions into common, i.e., present in both replicas, and unknown, i.e., present
        //   remotely but not locally.
        let (common, local_unkown): (Vec<_>, Vec<_>) =
            self.remote.split().into_iter().partition(|delta| {
                let item = delta
                    .elements()
                    .iter()
                    .next()
                    .expect("a decomposition should have a single item");
                local_filter.contains(item)
            });

        // 2.2. From the common partions build a Bloom Filter.
        let mut remote_filter = BloomFilter::with_capacity_and_fpr(common.len(), FPR);
        common.into_iter().for_each(|delta| {
            let item = delta
                .elements()
                .iter()
                .next()
                .expect("a decomposition should have a single item");
            remote_filter.insert(item);
        });

        // 2.3. Send back to the remote replica the unknown decompositions and the Bloom Filter.
        let remote_filter_len = remote_filter.as_bitslice().len();
        tracker.register(NetworkHop::RemoteToLocal(
            (remote_filter_len / 8 + min(1, remote_filter_len % 8))
                + 2 * std::mem::size_of::<RandomState>(),
        ));

        // 3.1. At the local replica, split the state into join decompositions. Then split the
        //   decompositions into common, i.e., present in both replicas, and unknown, i.e., present
        //   locally but not remotely.
        let (_, remote_unknown): (Vec<_>, Vec<_>) = local_split.into_iter().partition(|delta| {
            let item = delta
                .elements()
                .iter()
                .next()
                .expect("a decomposition should have a single item");
            remote_filter.contains(item)
        });

        // 3.2. Join the incoming local unknown decompositons.
        self.local.join(local_unkown);

        // 3.3. Send to the remote replica the unkown decompositions.
        tracker.register(NetworkHop::LocalToRemote(
            remote_unknown.iter().map(Probabilistic::size_of).sum(),
        ));

        // 4. Join the incoming remote unkown decompositions.
        self.remote.join(remote_unknown);

        // 5. Sanity check.
        // NOTE: This algorithm does not guarantee full state sync between replicas.
        tracker.finish(
            self.local
                .elements()
                .symmetric_difference(self.remote.elements())
                .count(),
        );

        tracker
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
    type Tracker = DefaultTracker;

    fn sync(&mut self) -> Self::Tracker {
        let mut tracker = Self::Tracker::new();

        const BUCKET: Vec<(GSet<String>, u64)> = Vec::new();

        let hasher = RandomState::new();
        let mut local_buckets = [BUCKET; B];

        // 1.1. Split the local state and assign each to a bucket. The assignment policy is
        //    implementation defined; here it is used the modulo of the hash value w.r.t. the
        //    number of buckets.
        self.local.split().into_iter().for_each(|delta| {
            let hash = hasher.hash_one(
                delta
                    .elements()
                    .iter()
                    .next()
                    .expect("a decomposition should have a single item"),
            );

            let i = usize::try_from(hash).unwrap() % local_buckets.len();
            local_buckets[i].push((delta, hash));
        });

        // 1.2. Sort the bucket's elements based on its items hash values, i.e, Merkle trick.
        local_buckets
            .iter_mut()
            .for_each(|bucket| bucket.sort_unstable_by_key(|k| k.1));

        // 1.3. At the local replica, compute the hash of each bucket.
        let local_hashes = local_buckets.iter().map(|bucket| {
            hasher.hash_one(
                bucket
                    .iter()
                    .fold(String::new(), |h, k| h + &k.1.to_string()),
            )
        });

        // 1.4 Send the bucket hashes to the remote replica.
        tracker.register(NetworkHop::LocalToRemote(std::mem::size_of::<u64>() * B));

        let mut remote_buckets = [BUCKET; B];

        // 2.1. Split the remote state and assign to a bucket. The assignment policy is
        //    implementation defined; here it is used the modulo of the hash value w.r.t. the
        //    number of buckets.
        //    NOTE: The policy must be same across replicas.
        self.remote.split().into_iter().for_each(|delta| {
            let hash = hasher.hash_one(
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
                let remote_hash = hasher.hash_one(
                    bucket
                        .iter()
                        .fold(String::new(), |h, k| h + &k.1.to_string()),
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
        tracker.register(NetworkHop::RemoteToLocal(
            non_matching_buckets
                .iter()
                .flatten()
                .map(BucketDispatcher::<B>::size_of)
                .sum(),
        ));

        // 3.1. Compute the state that has been not yet seen by the local replica, and send back to
        //    remote peer the state that he has not seen yet. Only the difference, i.e., the
        //    optimal delta, computed from decompositions, is sent back.
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
            .fold((vec![], vec![]), |mut unseen, buckets| {
                unseen.0.push(buckets.1.difference(&buckets.0));
                unseen.1.push(buckets.0.difference(&buckets.1));

                unseen
            });

        // 3.2. Join the buckets received from the remote replica that contain some state.
        self.local.join(local_unseen);

        tracker.register(NetworkHop::LocalToRemote(
            remote_unseen
                .iter()
                .map(BucketDispatcher::<B>::size_of)
                .sum(),
        ));

        // 4.1. Join the optimal deltas received from the local replica.
        self.remote.join(remote_unseen);

        // 5. Sanity check.
        // NOTE: This algorithm guarantees that replicas sync given that no operations occur.
        tracker.finish(
            self.local
                .elements()
                .symmetric_difference(self.remote.elements())
                .count(),
        );

        tracker
    }

    fn size_of(replica: &Self::Replica) -> usize {
        replica.elements().iter().map(String::len).sum()
    }

    fn is_synced(&self) -> bool {
        self.local == self.remote
    }
}
