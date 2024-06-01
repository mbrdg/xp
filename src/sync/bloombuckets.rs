use std::{collections::BTreeMap, hash::BuildHasher, hash::RandomState, iter::zip};

use crate::{
    bloom::BloomFilter,
    crdt::{Decomposable, GSet},
    tracker::{DefaultTracker, NetworkEvent, Tracker},
};

use super::{BuildBloomFilters, BuildBuckets, BuildProtocol, Protocol, ReplicaSize};

pub struct BloomBuckets {
    local: GSet<String>,
    remote: GSet<String>,
    fpr: f64,
    hasher: RandomState,
    load_factor: f64,
}

pub struct BloomBucketsBuilder {
    local: GSet<String>,
    remote: GSet<String>,
    fpr: Option<f64>,
    hasher: Option<RandomState>,
    load_factor: Option<f64>,
}

impl BloomBucketsBuilder {
    pub fn fpr(mut self, fpr: f64) -> Self {
        assert!(
            (0.0..1.0).contains(&fpr) && fpr > 0.0,
            "false positive rate should be a ratio greater than 0.0"
        );

        self.fpr = Some(fpr);
        self
    }

    pub fn load_factor(mut self, load_factor: f64) -> Self {
        assert!(load_factor > 0.0, "load factor should be greater than 0.0");

        self.load_factor = Some(load_factor);
        self
    }

    pub fn hasher(mut self, hasher: RandomState) -> Self {
        self.hasher = Some(hasher);
        self
    }
}

impl BuildProtocol for BloomBucketsBuilder {
    type Protocol = BloomBuckets;

    fn build(self) -> Self::Protocol {
        BloomBuckets {
            local: self.local,
            remote: self.remote,
            fpr: self.fpr.unwrap_or(0.01),
            hasher: self.hasher.unwrap_or_default(),
            load_factor: self.load_factor.unwrap_or(1.0),
        }
    }
}

impl BuildBloomFilters for BloomBuckets {
    type Decomposition = GSet<String>;
    type Item = String;

    fn filter(decompositions: &[Self::Decomposition], fpr: f64) -> BloomFilter<Self::Item> {
        let mut filter = BloomFilter::new(decompositions.len(), fpr);
        decompositions.iter().for_each(|delta| {
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

impl BuildBuckets for BloomBuckets {
    type Replica = GSet<String>;
    type Hasher = RandomState;
    type Bucket = BTreeMap<u64, GSet<String>>;

    fn buckets(
        replica: &Self::Replica,
        hasher: &Self::Hasher,
        num_buckets: usize,
    ) -> Vec<Self::Bucket> {
        let mut buckets = vec![Self::Bucket::new(); num_buckets];
        replica.split().into_iter().for_each(|delta| {
            let item = delta
                .elements()
                .iter()
                .next()
                .expect("a decomposition should have a single item");
            let hash = hasher.hash_one(item);
            let idx = usize::try_from(hash).unwrap() % buckets.len();

            buckets[idx].insert(hash, delta);
        });

        buckets
    }

    fn hashes(buckets: &[Self::Bucket], hasher: &Self::Hasher) -> Vec<u64> {
        buckets
            .iter()
            .map(|bucket| {
                let fingerprint = bucket
                    .keys()
                    .fold(String::new(), |acc, f| format!("{acc}{f}"));
                hasher.hash_one(fingerprint)
            })
            .collect()
    }
}

impl ReplicaSize for BloomBuckets {
    type Replica = GSet<String>;

    fn size_of(replica: &Self::Replica) -> usize {
        replica.elements().iter().map(String::len).sum()
    }
}

impl Protocol for BloomBuckets {
    type Replica = GSet<String>;
    type Builder = BloomBucketsBuilder;
    type Tracker = DefaultTracker;

    fn builder(local: Self::Replica, remote: Self::Replica) -> Self::Builder {
        BloomBucketsBuilder {
            local,
            remote,
            fpr: None,
            hasher: None,
            load_factor: None,
        }
    }

    fn sync(&mut self, tracker: &mut Self::Tracker) {
        assert!(
            tracker.is_ready(),
            "tracker should be ready, i.e., no captured events and not finished"
        );

        // 1. Build, locally, the AMQ filter from the local splitted decompositions.
        let local_decompositions = self.local.split();
        let local_filter = BloomBuckets::filter(&local_decompositions, self.fpr);

        // 1.1. Ship the filter to the remote replica.
        tracker.register(NetworkEvent::local_to_remote(
            tracker.upload(),
            <BloomBuckets as BuildBloomFilters>::size_of(&local_filter),
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
        let remote_filter = BloomBuckets::filter(&common, self.fpr);
        let remote_buckets = {
            let mut state = GSet::new();
            state.join(common);

            let num_buckets = (state.len() as f64 * self.load_factor) as usize;
            BloomBuckets::buckets(&state, &self.hasher, num_buckets)
        };
        let remote_hashes = BloomBuckets::hashes(&remote_buckets, &self.hasher);

        // 2.2. Ship the local unknown decompositions, the AMQ filter and the bucket's hashes.
        tracker.register(NetworkEvent::remote_to_local(
            tracker.download(),
            <BloomBuckets as BuildBloomFilters>::size_of(&remote_filter)
                + std::mem::size_of::<u64>() * remote_hashes.len()
                + local_unknown
                    .iter()
                    .map(<BloomBuckets as ReplicaSize>::size_of)
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
            BloomBuckets::buckets(&state, &self.hasher, remote_hashes.len())
        };
        let local_hashes = BloomBuckets::hashes(&local_buckets, &self.hasher);

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
                    .map(<BloomBuckets as ReplicaSize>::size_of)
                    .sum::<usize>()
                + remote_unknown
                    .iter()
                    .map(<BloomBuckets as ReplicaSize>::size_of)
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
            zip(remote_buckets, non_matching_buckets)
                .flat_map(|(remote, local)| remote.zip(local))
                .map(|(remote, local)| (local.difference(&remote), remote.difference(&local)))
                .unzip();

        let joinable = Vec::from_iter(remote_unknown.into_iter().chain(remote_escaped));
        self.remote.join(joinable);

        // 4.1 Ship back the difference from the common decompositions which were false positives
        //   to the AMQ filters.
        tracker.register(NetworkEvent::remote_to_local(
            tracker.download(),
            local_escaped
                .iter()
                .map(<BloomBuckets as ReplicaSize>::size_of)
                .sum(),
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tracker::NetworkBandwitdth;

    #[test]
    fn test_sync() {
        let local = {
            let mut gset = GSet::<String>::new();
            let items = "Stuck In A Moment You Can't Get Out Of"
                .split_whitespace()
                .collect::<Vec<_>>();

            for item in items {
                gset.insert(item.to_string());
            }

            gset
        };

        let remote = {
            let mut gset = GSet::<String>::new();
            let items = "I Still Haven't Found What I'm Looking For"
                .split_whitespace()
                .collect::<Vec<_>>();

            for item in items {
                gset.insert(item.to_string());
            }

            gset
        };

        let mut baseline = BloomBuckets::builder(local, remote).build();
        let (download, upload) = (NetworkBandwitdth::Kbps(0.5), NetworkBandwitdth::Kbps(0.5));
        let mut tracker = DefaultTracker::new(download, upload);

        baseline.sync(&mut tracker);
        assert_eq!(tracker.diffs(), 0);

        let events = tracker.events();
        assert_eq!(events.len(), 4);
    }
}
