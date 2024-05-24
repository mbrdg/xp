use std::{
    cmp::min,
    collections::BTreeMap,
    hash::{BuildHasher, RandomState},
    iter::zip,
};

use crate::{
    bloom::BloomFilter,
    crdt::{Decomposable, GSet},
    tracker::{DefaultTracker, NetworkEvent, Tracker},
};

pub trait Protocol {
    type Replica: Decomposable;
    type Tracker: Tracker;

    fn sync(&mut self, tracker: &mut Self::Tracker);
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

impl Protocol for Baseline {
    type Replica = GSet<String>;
    type Tracker = DefaultTracker;

    fn sync(&mut self, tracker: &mut Self::Tracker) {
        assert!(
            tracker.events().is_empty() && tracker.diffs().is_none(),
            "tracker should be empty and not finished"
        );

        // 1. Ship the full local state and send it the remote replica.
        let local_state = self.local.clone();

        tracker.register(NetworkEvent::local_to_remote(
            tracker.upload(),
            Baseline::size_of(&local_state),
        ));

        // 2.1. Compute the optimal delta based on the remote replica state.
        let remote_unseen = local_state.difference(&self.remote);
        let local_unseen = self.remote.difference(&local_state);

        // 2.2. Join the decompositions that are unknown to the remote replica.
        self.remote.join(vec![remote_unseen]);

        // 2.3. Send back to the local replica the decompositions unknown to the local replica.
        tracker.register(NetworkEvent::remote_to_local(
            tracker.download(),
            Baseline::size_of(&local_unseen),
        ));

        // 3. Merge the minimum delta received from the remote replica.
        self.local.join(vec![local_unseen]);

        // 4. Sanity check.
        tracker.finish(
            self.local
                .elements()
                .symmetric_difference(self.remote.elements())
                .count(),
        );

        // NOTE: This algorithm guarantees that replicas sync given that no operations occur.
        assert!(self.is_synced());
    }

    fn size_of(replica: &Self::Replica) -> usize {
        replica.elements().iter().map(String::len).sum()
    }

    fn is_synced(&self) -> bool {
        self.local == self.remote
    }
}

pub struct BloomBased {
    local: GSet<String>,
    remote: GSet<String>,
    fpr: f64,
}

impl BloomBased {
    #[inline]
    #[must_use]
    pub fn new(local: GSet<String>, remote: GSet<String>) -> Self {
        Self {
            local,
            remote,
            fpr: 0.01 / 100.0, // 0.01%
        }
    }

    #[inline]
    #[must_use]
    pub fn with_fpr(local: GSet<String>, remote: GSet<String>, fpr: f64) -> Self {
        assert!(
            (0.0..1.0).contains(&fpr) && fpr != 0.0,
            "false positive rate should be in the interval [0.0 and 1.0)"
        );

        Self { local, remote, fpr }
    }

    #[inline]
    fn size_of_filter(filter: &BloomFilter<String>) -> usize {
        // TODO: assume a language agnostic attitude by remove the idiosyncrasies of rust, namely,
        // `RandomState` to ensure that the replicas hash deterministically.
        let len = filter.as_bitslice().len();
        len / 8 + min(1, len % 8) + 2 * std::mem::size_of::<RandomState>()
    }
}

impl Protocol for BloomBased {
    type Replica = GSet<String>;
    type Tracker = DefaultTracker;

    fn sync(&mut self, tracker: &mut Self::Tracker) {
        assert!(
            tracker.events().is_empty() && tracker.diffs().is_none(),
            "tracker should be empty and not finished"
        );

        // 1.1. Split the local state and insert each decomposition into a Bloom Filter.
        let local_split = self.local.split();
        let mut local_filter = BloomFilter::new(local_split.len(), self.fpr);

        local_split.iter().for_each(|delta| {
            let item = delta
                .elements()
                .iter()
                .next()
                .expect("a decomposition should have a single element");
            local_filter.insert(item);
        });

        // 1.2. Ship the Bloom filter to the remote replica.
        tracker.register(NetworkEvent::local_to_remote(
            tracker.upload(),
            BloomBased::size_of_filter(&local_filter),
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
        let mut remote_filter = BloomFilter::new(common.len(), self.fpr);
        common.into_iter().for_each(|delta| {
            let item = delta
                .elements()
                .iter()
                .next()
                .expect("a decomposition should have a single item");
            remote_filter.insert(item);
        });

        // 2.3. Send back to the remote replica the unknown decompositions and the Bloom Filter.
        tracker.register(NetworkEvent::remote_to_local(
            tracker.download(),
            BloomBased::size_of_filter(&remote_filter)
                + local_unkown.iter().map(BloomBased::size_of).sum::<usize>(),
        ));

        // 3.1. At the local replica, split the state into join decompositions. Then split the
        //   decompositions into common, i.e., present in both replicas, and unknown, i.e., present
        //   locally but not remotely.
        let remote_unknown: Vec<_> = local_split
            .into_iter()
            .partition(|delta| {
                let item = delta
                    .elements()
                    .iter()
                    .next()
                    .expect("a decomposition should have a single item");
                remote_filter.contains(item)
            })
            .1;

        // 3.2. Join the incoming local unknown decompositons.
        self.local.join(local_unkown);

        // 3.3. Send to the remote replica the unkown decompositions.
        tracker.register(NetworkEvent::local_to_remote(
            tracker.upload(),
            remote_unknown.iter().map(BloomBased::size_of).sum(),
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
    }

    fn size_of(replica: &Self::Replica) -> usize {
        replica.elements().iter().map(String::len).sum()
    }

    fn is_synced(&self) -> bool {
        self.local == self.remote
    }
}

type Bucket<T> = BTreeMap<u64, T>;
pub struct Buckets<const B: usize> {
    local: GSet<String>,
    remote: GSet<String>,
    hasher: RandomState,
}

impl<const B: usize> Buckets<B> {
    #[inline]
    #[must_use]
    pub fn new(local: GSet<String>, remote: GSet<String>) -> Self {
        Self {
            local,
            remote,
            hasher: RandomState::new(),
        }
    }

    fn build_buckets(
        replica: &GSet<String>,
        hasher: &RandomState,
        len: usize,
    ) -> Vec<Bucket<GSet<String>>> {
        let mut buckets = vec![BTreeMap::new(); len];

        replica.split().into_iter().for_each(|delta| {
            let item = delta
                .elements()
                .iter()
                .next()
                .expect("a decomposition should have a single item");
            let hash = hasher.hash_one(item);
            let index = usize::try_from(hash).unwrap() % buckets.len();

            buckets[index].insert(hash, delta);
        });

        buckets
    }

    fn build_hashes(buckets: &[BTreeMap<u64, GSet<String>>], hasher: &RandomState) -> Vec<u64> {
        buckets
            .iter()
            .map(|bucket| {
                hasher.hash_one(
                    bucket
                        .keys()
                        .fold(String::new(), |acc, h| format!("{acc}{h}")),
                )
            })
            .collect()
    }
}

impl<const B: usize> Protocol for Buckets<B> {
    type Replica = GSet<String>;
    type Tracker = DefaultTracker;

    fn sync(&mut self, tracker: &mut Self::Tracker) {
        assert!(
            tracker.events().is_empty() && tracker.diffs().is_none(),
            "tracker should be empty and not finished"
        );

        // 1.1. Split the local state and assign each to a bucket. The assignment policy is
        //    implementation defined; here it is used the modulo of the hash value w.r.t. the
        //    number of buckets.
        let local_buckets = Buckets::<B>::build_buckets(&self.local, &self.hasher, B);
        let local_hashes = Buckets::<B>::build_hashes(&local_buckets, &self.hasher);

        // 1.2 Send the bucket hashes to the remote replica.
        tracker.register(NetworkEvent::local_to_remote(
            tracker.upload(),
            std::mem::size_of::<u64>() * local_hashes.len(),
        ));

        // 2.1. Split the remote state and assign to a bucket. The assignment policy is
        //    implementation defined; here it is used the modulo of the hash value w.r.t. the
        //    number of buckets.
        //    NOTE: The policy must be same across replicas.
        let remote_buckets = Buckets::<B>::build_buckets(&self.remote, &self.hasher, B);
        let remote_hashes = Buckets::<B>::build_hashes(&remote_buckets, &self.hasher);

        // 2.3. Aggregate the state from the non matching buckets. Or, if the hashes match, ship
        //   back an empty payload to the local replica.
        let matchings = zip(local_hashes, remote_hashes).map(|(local, remote)| local == remote);
        let non_matching_buckets: Vec<_> = zip(remote_buckets, matchings)
            .map(|(bucket, matching)| {
                if matching {
                    None
                } else {
                    let mut state = GSet::new();
                    state.join(Vec::from_iter(bucket.into_values()));
                    Some(state)
                }
            })
            .collect();

        // 2.4. Send the aggregated bucket state back to the local replica.
        tracker.register(NetworkEvent::remote_to_local(
            tracker.download(),
            non_matching_buckets.len()
                + non_matching_buckets
                    .iter()
                    .flatten()
                    .map(Buckets::<B>::size_of)
                    .sum::<usize>(),
        ));

        // 3.1. Compute the state that has been not yet seen by the local replica, and send back to
        //    remote peer the state that he has not seen yet. Only the difference, i.e., the
        //    optimal delta, computed from decompositions, is sent back.
        let local_buckets = local_buckets.into_iter().map(|bucket| {
            let mut state = GSet::new();
            state.join(Vec::from_iter(bucket.into_values()));
            Some(state)
        });

        let (local_unseen, remote_unseen): (Vec<_>, Vec<_>) =
            zip(local_buckets, non_matching_buckets)
                .flat_map(|(local_bucket, remote_bucket)| local_bucket.zip(remote_bucket))
                .map(|(local, remote)| (remote.difference(&local), local.difference(&remote)))
                .unzip();

        // 3.2. Join the buckets received from the remote replica that contain some state.
        self.local.join(local_unseen);

        tracker.register(NetworkEvent::local_to_remote(
            tracker.upload(),
            remote_unseen.iter().map(Buckets::<B>::size_of).sum(),
        ));

        // 4.1. Join the optimal deltas received from the local replica.
        self.remote.join(remote_unseen);

        // 5. Sanity check.
        tracker.finish(
            self.local
                .elements()
                .symmetric_difference(self.remote.elements())
                .count(),
        );

        // NOTE: This algorithm guarantees that replicas sync given that no operations occur.
        assert!(self.is_synced());
    }

    fn size_of(replica: &Self::Replica) -> usize {
        replica.elements().iter().map(String::len).sum()
    }

    fn is_synced(&self) -> bool {
        self.local == self.remote
    }
}
