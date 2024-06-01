use std::{collections::BTreeMap, hash::BuildHasher, hash::RandomState, iter::zip};

use crate::{
    crdt::{gset::GSet, Decomposable},
    tracker::{DefaultTracker, NetworkEvent, Tracker},
};

use super::{BuildBuckets, BuildProtocol, Protocol, ReplicaSize};

pub struct Buckets {
    local: GSet<String>,
    remote: GSet<String>,
    hasher: RandomState,
    num_buckets: usize,
}

pub struct BucketsBuilder {
    local: GSet<String>,
    remote: GSet<String>,
    hasher: Option<RandomState>,
    load_factor: Option<f64>,
}

impl BucketsBuilder {
    fn load_factor(mut self, load_factor: f64) -> Self {
        assert!(load_factor > 0.0, "load factor should be greater than 0.0");

        self.load_factor = Some(load_factor);
        self
    }

    fn hasher(mut self, hasher: RandomState) -> Self {
        self.hasher = Some(hasher);
        self
    }
}

impl BuildProtocol for BucketsBuilder {
    type Protocol = Buckets;

    fn build(self) -> Self::Protocol {
        let load_factor = self.load_factor.unwrap_or(1.0);
        let len = self.local.len();

        Buckets {
            local: self.local,
            remote: self.remote,
            hasher: self.hasher.unwrap_or_default(),
            num_buckets: (len as f64 * load_factor) as usize,
        }
    }
}

impl ReplicaSize for Buckets {
    type Replica = GSet<String>;

    fn size_of(replica: &Self::Replica) -> usize {
        replica.elements().iter().map(String::len).sum()
    }
}

impl BuildBuckets for Buckets {
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

impl Protocol for Buckets {
    type Replica = GSet<String>;
    type Builder = BucketsBuilder;
    type Tracker = DefaultTracker;

    fn builder(local: Self::Replica, remote: Self::Replica) -> Self::Builder {
        BucketsBuilder {
            local,
            remote,
            hasher: None,
            load_factor: None,
        }
    }

    fn sync(&mut self, tracker: &mut Self::Tracker) {
        assert!(
            tracker.is_ready(),
            "tracker should be ready, i.e., no captured events and not finished"
        );

        // 1.1. Split the local state and assign each to a bucket. The assignment policy is
        //    implementation defined; here it is used the modulo of the hash value w.r.t. the
        //    number of buckets.
        let local_buckets = Buckets::buckets(&self.local, &self.hasher, self.num_buckets);
        let local_hashes = Buckets::hashes(&local_buckets, &self.hasher);

        // 1.2 Send the bucket hashes to the remote replica.
        tracker.register(NetworkEvent::local_to_remote(
            tracker.upload(),
            std::mem::size_of::<u64>() * local_hashes.len(),
        ));

        // 2.1. Split the remote state and assign to a bucket. The assignment policy is
        //    implementation defined; here it is used the modulo of the hash value w.r.t. the
        //    number of buckets.
        //    NOTE: The policy must be same across replicas.
        let remote_buckets = Buckets::buckets(&self.remote, &self.hasher, self.num_buckets);
        let remote_hashes = Buckets::hashes(&remote_buckets, &self.hasher);

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
                .flat_map(|(local, remote)| local.zip(remote))
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
}

#[cfg(test)]
mod tests {
    use std::mem;

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

        let mut baseline = Buckets::builder(local, remote).load_factor(1.25).build();
        let (download, upload) = (NetworkBandwitdth::Kbps(0.5), NetworkBandwitdth::Kbps(0.5));
        let mut tracker = DefaultTracker::new(download, upload);

        baseline.sync(&mut tracker);

        let bytes: Vec<_> = tracker.events().iter().map(NetworkEvent::bytes).collect();
        assert_eq!(bytes, vec![11 * mem::size_of::<u64>(), 11 + 35, 30]);
        assert_eq!(tracker.diffs(), 0);
    }
}
