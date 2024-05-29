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
    }

    fn size_of(replica: &Self::Replica) -> usize {
        replica.elements().iter().map(String::len).sum()
    }
}

pub struct Bloom {
    local: GSet<String>,
    remote: GSet<String>,
    fpr: f64,
}

impl Bloom {
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
        let len = filter.as_bitslice().len();
        len / 8 + min(1, len % 8) + 2 * std::mem::size_of::<RandomState>()
    }

    fn build_filter(slice: &[GSet<String>], fpr: f64) -> BloomFilter<String> {
        let mut filter = BloomFilter::new(slice.len(), fpr);
        slice.iter().for_each(|delta| {
            let item = delta
                .elements()
                .iter()
                .next()
                .expect("a decomposition should have a single item");
            filter.insert(item);
        });

        filter
    }
}

impl Protocol for Bloom {
    type Replica = GSet<String>;
    type Tracker = DefaultTracker;

    fn sync(&mut self, tracker: &mut Self::Tracker) {
        assert!(
            tracker.events().is_empty() && tracker.diffs().is_none(),
            "tracker should be empty and not finished"
        );

        // 1.1. Split the local state and insert each decomposition into a Bloom Filter.
        let local_split = self.local.split();
        let local_filter = Bloom::build_filter(&local_split, self.fpr);

        // 1.2. Ship the Bloom filter to the remote replica.
        tracker.register(NetworkEvent::local_to_remote(
            tracker.upload(),
            Bloom::size_of_filter(&local_filter),
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
        let remote_filter = Bloom::build_filter(common.as_slice(), self.fpr);

        // 2.3. Send back to the remote replica the unknown decompositions and the Bloom Filter.
        tracker.register(NetworkEvent::remote_to_local(
            tracker.download(),
            Bloom::size_of_filter(&remote_filter)
                + local_unkown.iter().map(Bloom::size_of).sum::<usize>(),
        ));

        // 3.1. At the local replica, split the state into join decompositions. Then split the
        //   decompositions into common, i.e., present in both replicas, and unknown, i.e., present
        //   locally but not remotely.
        let remote_unknown: Vec<_> = local_split
            .into_iter()
            .filter(|delta| {
                let item = delta
                    .elements()
                    .iter()
                    .next()
                    .expect("a decomposition should have a single item");
                !remote_filter.contains(item)
            })
            .collect();

        // 3.2. Join the incoming local unknown decompositons.
        self.local.join(local_unkown);

        // 3.3. Send to the remote replica the unkown decompositions.
        tracker.register(NetworkEvent::local_to_remote(
            tracker.upload(),
            remote_unknown.iter().map(Bloom::size_of).sum(),
        ));

        // 4. Join the incoming remote unkown decompositions.
        self.remote.join(remote_unknown);

        // 5. Sanity check.
        // WARN: This algorithm does not guarantee full state sync between replicas.
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
}

type Bucket<T> = BTreeMap<u64, T>;
pub struct Buckets {
    local: GSet<String>,
    remote: GSet<String>,
    hasher: RandomState,
    b: usize,
}

impl Buckets {
    #[inline]
    #[must_use]
    pub fn new(local: GSet<String>, remote: GSet<String>) -> Self {
        Self {
            b: local.len(),
            local,
            remote,
            hasher: RandomState::new(),
        }
    }

    #[inline]
    #[must_use]
    pub fn with_load_factor(local: GSet<String>, remote: GSet<String>, load_factor: f64) -> Self {
        assert!(load_factor >= 0.0, "load factor should be greater than 0.0");

        Self {
            b: (local.len() as f64 * load_factor) as usize,
            local,
            remote,
            hasher: RandomState::new(),
        }
    }

    fn build_buckets(
        replica: &GSet<String>,
        hasher: &RandomState,
        num_buckets: usize,
    ) -> Vec<Bucket<GSet<String>>> {
        let mut buckets = vec![Bucket::new(); num_buckets];
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

    fn build_hashes(buckets: &[Bucket<GSet<String>>], hasher: &RandomState) -> Vec<u64> {
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

impl Protocol for Buckets {
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
        let local_buckets = Buckets::build_buckets(&self.local, &self.hasher, self.b);
        let local_hashes = Buckets::build_hashes(&local_buckets, &self.hasher);

        // 1.2 Send the bucket hashes to the remote replica.
        tracker.register(NetworkEvent::local_to_remote(
            tracker.upload(),
            std::mem::size_of::<u64>() * local_hashes.len(),
        ));

        // 2.1. Split the remote state and assign to a bucket. The assignment policy is
        //    implementation defined; here it is used the modulo of the hash value w.r.t. the
        //    number of buckets.
        //    NOTE: The policy must be same across replicas.
        let remote_buckets = Buckets::build_buckets(&self.remote, &self.hasher, self.b);
        let remote_hashes = Buckets::build_hashes(&remote_buckets, &self.hasher);

        // 2.3. Aggregate the state from the non matching buckets. Or, if the hashes match, ship
        //   back an empty payload to the local replica.
        let matchings = zip(local_hashes, remote_hashes).map(|(local, remote)| local == remote);
        let non_matching_buckets: Vec<_> = zip(remote_buckets, matchings)
            .map(|(bucket, matching)| {
                (!matching).then(|| {
                    let mut state = GSet::new();
                    state.join(Vec::from_iter(bucket.into_values()));
                    state
                })
            })
            .collect();

        // 2.4. Send the aggregated bucket state back to the local replica.
        tracker.register(NetworkEvent::remote_to_local(
            tracker.download(),
            non_matching_buckets.len()
                + non_matching_buckets
                    .iter()
                    .flatten()
                    .map(Buckets::size_of)
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
            remote_unseen.iter().map(Buckets::size_of).sum(),
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
    }

    fn size_of(replica: &Self::Replica) -> usize {
        replica.elements().iter().map(String::len).sum()
    }
}

pub struct BloomBuckets {
    local: GSet<String>,
    remote: GSet<String>,
    hasher: RandomState,
    fpr: f64,
}

impl BloomBuckets {
    #[inline]
    #[must_use]
    pub fn new(local: GSet<String>, remote: GSet<String>) -> Self {
        Self {
            local,
            remote,
            hasher: RandomState::new(),
            fpr: 0.0001 / 100.0, // 1%
        }
    }
}

impl BloomBuckets {
    fn build_buckets(
        replica: &GSet<String>,
        hasher: &RandomState,
        num_buckets: usize,
    ) -> Vec<Bucket<GSet<String>>> {
        let mut buckets = vec![Bucket::new(); num_buckets];
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

    fn build_hashes(buckets: &[Bucket<GSet<String>>], hasher: &RandomState) -> Vec<u64> {
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

    fn build_filter(slice: &[GSet<String>], fpr: f64) -> BloomFilter<String> {
        let mut filter = BloomFilter::new(slice.len(), fpr);
        slice.iter().for_each(|delta| {
            let item = delta
                .elements()
                .iter()
                .next()
                .expect("a decomposition should have a single item");
            filter.insert(item);
        });

        filter
    }

    #[inline]
    fn size_of_filter(filter: &BloomFilter<String>) -> usize {
        let len = filter.as_bitslice().len();
        len / 8 + min(1, len % 8) + 2 * std::mem::size_of::<RandomState>()
    }
}

impl Protocol for BloomBuckets {
    type Replica = GSet<String>;
    type Tracker = DefaultTracker;

    fn sync(&mut self, tracker: &mut Self::Tracker) {
        assert!(
            tracker.events().is_empty() && tracker.diffs().is_none(),
            "tracker should be empty and not finished"
        );

        // 1. Build, locally, the AMQ filter from the local splitted decompositions.
        let local_decompositions = self.local.split();
        let local_filter = BloomBuckets::build_filter(&local_decompositions, self.fpr);

        // 1.1. Ship the filter to the remote replica.
        tracker.register(NetworkEvent::local_to_remote(
            tracker.upload(),
            BloomBuckets::size_of_filter(&local_filter),
        ));

        // 2. Remotely, divide decompositions into common, i.e., probably in both replicas and into
        //    local_unkown, i.e., decompositions that are not definetly at the local replica.
        let (common, local_unknown): (Vec<_>, Vec<_>) =
            self.remote.split().into_iter().partition(|delta| {
                let item = delta
                    .elements()
                    .iter()
                    .next()
                    .expect("a decomposition should have a single item");
                local_filter.contains(item)
            });

        // 2.1. Build a AMQ filter and the corresponding buckets from the common decompositions.
        let remote_filter = BloomBuckets::build_filter(&common, self.fpr);
        let remote_buckets = {
            let mut state = GSet::new();
            state.join(common);
            BloomBuckets::build_buckets(&state, &self.hasher, state.len())
        };
        let remote_hashes = BloomBuckets::build_hashes(&remote_buckets, &self.hasher);

        // 2.2. Ship the local unknown decompositions, the AMQ filter and the bucket's hashes.
        tracker.register(NetworkEvent::remote_to_local(
            tracker.download(),
            BloomBuckets::size_of_filter(&remote_filter)
                + std::mem::size_of::<u64>() * remote_hashes.len()
                + local_unknown
                    .iter()
                    .map(BloomBuckets::size_of)
                    .sum::<usize>(),
        ));

        // 3. Locally, divide decompositions into common, i.e., probably in both replicas and into
        //    local_unkown, i.e., decompositions that are not definetly at the remote replica.
        let (common, remote_unknown): (Vec<_>, Vec<_>) =
            local_decompositions.into_iter().partition(|delta| {
                let item = delta
                    .elements()
                    .iter()
                    .next()
                    .expect("a decomposition should have a single item");
                remote_filter.contains(item)
            });

        // 3.1. Build the local buckets from the common decompositions.
        let local_buckets = {
            let mut state = GSet::new();
            state.join(common);
            BloomBuckets::build_buckets(&state, &self.hasher, remote_hashes.len())
        };
        let local_hashes = BloomBuckets::build_hashes(&local_buckets, &self.hasher);

        // 3.2. Determine which buckets do not match with the remote replica.
        let matchings = zip(local_hashes, remote_hashes).map(|(local, remote)| local == remote);
        let non_matching_buckets: Vec<_> = zip(local_buckets, matchings)
            .map(|(bucket, matching)| {
                (!matching).then(|| {
                    let mut state = GSet::new();
                    state.join(Vec::from_iter(bucket.into_values()));
                    state
                })
            })
            .collect();

        // 3.3. Ship the state that doesn't match to the remote replica.
        tracker.register(NetworkEvent::local_to_remote(
            tracker.upload(),
            non_matching_buckets.len()
                + non_matching_buckets
                    .iter()
                    .flatten()
                    .map(BloomBuckets::size_of)
                    .sum::<usize>()
                + remote_unknown
                    .iter()
                    .map(BloomBuckets::size_of)
                    .sum::<usize>(),
        ));

        // 4. Compute back the difference from the non matching buckets
        let remote_buckets = remote_buckets.into_iter().map(|bucket| {
            let mut state = GSet::new();
            state.join(Vec::from_iter(bucket.into_values()));
            Some(state)
        });

        // These vectors yield the decompositions that were false positives.
        let (remote_escaped, local_escaped): (Vec<_>, Vec<_>) =
            zip(non_matching_buckets, remote_buckets)
                .flat_map(|(local, remote)| remote.zip(local))
                .map(|(remote, local)| (local.difference(&remote), remote.difference(&local)))
                .unzip();

        let joinable = Vec::from_iter(remote_unknown.into_iter().chain(remote_escaped));
        self.remote.join(joinable);

        // 4.1 Ship back the difference from the common decompositions which were false positives
        //   to the AMQ filters.
        tracker.register(NetworkEvent::remote_to_local(
            tracker.download(),
            local_escaped.iter().map(BloomBuckets::size_of).sum(),
        ));

        // 5. Merge the collected differences at both replicas.
        let joinable = Vec::from_iter(local_unknown.into_iter().chain(local_escaped));
        self.local.join(joinable);

        // 6. Sanity Check.
        tracker.finish(
            self.local
                .elements()
                .symmetric_difference(self.remote.elements())
                .count(),
        );

        if tracker.diffs().is_some_and(|d| d > 0) {
            for i in self
                .local
                .elements()
                .symmetric_difference(self.remote.elements())
            {
                dbg!(
                    i,
                    self.local.elements().contains(i),
                    self.remote.elements().contains(i),
                    local_filter.contains(i),
                    remote_filter.contains(i),
                );
            }
        }
    }

    fn size_of(replica: &Self::Replica) -> usize {
        replica.elements().iter().map(String::len).sum()
    }
}
