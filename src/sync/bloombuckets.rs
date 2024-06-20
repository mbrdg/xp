use std::{collections::HashMap, hash::RandomState, iter::zip, mem};

use crate::{
    crdt::{Decompose, Extract, Measure},
    tracker::{DefaultEvent, DefaultTracker, Telemetry},
};

use super::{Algorithm, BuildFilter, Dispatcher};

pub struct BloomBuckets<T> {
    local: T,
    remote: T,
    fpr: f64,
    buckets: usize,
}

impl<T> BloomBuckets<T> {
    #[inline]
    #[must_use]
    pub fn new(local: T, remote: T, fpr: f64, buckets: usize) -> Self {
        Self {
            local,
            remote,
            fpr,
            buckets,
        }
    }
}

impl<T> BuildFilter<T> for BloomBuckets<T> where T: Extract {}
impl<T> Dispatcher<T> for BloomBuckets<T> where T: Clone + Decompose<Decomposition = T> + Extract {}

impl<T> Algorithm for BloomBuckets<T>
where
    T: Clone + Decompose<Decomposition = T> + Default + Extract + Measure,
{
    type Tracker = DefaultTracker;

    fn sync(&mut self, tracker: &mut Self::Tracker) {
        assert!(
            tracker.is_ready(),
            "tracker should be ready, i.e., no captured events and not finished"
        );

        let hasher = RandomState::new();

        // 1. Create a filter from the local join-deocompositions and send it to the remote replica.
        let local_decompositions = self.local.split();
        let local_filter = self.filter_from(&local_decompositions, self.fpr);

        tracker.register(DefaultEvent::LocalToRemote {
            state: 0,
            metadata: <Self as BuildFilter<T>>::size_of(&local_filter),
            upload: tracker.upload(),
        });

        // 2. Partion the remote join-decompositions into *probably* present in both replicas or
        //    *definitely not* present in the local replica.
        let (remote_common, local_unknown) = self.partition(&local_filter, self.remote.split());

        // 3. Build a filter from the partion of *probably* common join-decompositions and send it
        //    to the local replica. At this stage the remote replica also constructs its buckets.
        //    For pipelining, the remaining decompositions and bucket's hashes are also sent.
        let remote_filter = self.filter_from(&remote_common, self.fpr);
        let remote_buckets = {
            let mut state = T::default();
            state.join(remote_common);

            self.dispatch(&state, self.buckets, &hasher)
        };
        let remote_hashes = BloomBuckets::<T>::hashes(&remote_buckets, &hasher);

        tracker.register(DefaultEvent::RemoteToLocal {
            state: local_unknown.iter().map(<T as Measure>::size_of).sum(),
            metadata: <Self as BuildFilter<T>>::size_of(&remote_filter)
                + mem::size_of_val(remote_hashes.as_slice()),
            download: tracker.download(),
        });

        // 3. Compute the buckets whose hash does not match on the local replica and send those
        //    buckets back to the remote replica together with all the decompositions that are
        //    *definitely not* on the remote replica.
        let (local_common, remote_unknown) = self.partition(&remote_filter, local_decompositions);

        // Assign each join-decomposition from the set of *probably* common join-decompositions to
        // a bucket based on the modulo of its hash and send the hashes to the remote replica.
        // NOTE: This policy must be deterministic across both peers.
        let local_buckets = {
            let mut state = T::default();
            state.join(local_common);

            self.dispatch(&state, self.buckets, &hasher)
        };
        let local_hashes = BloomBuckets::<T>::hashes(&local_buckets, &hasher);

        let non_matching = local_buckets
            .into_iter()
            .enumerate()
            .zip(zip(local_hashes, remote_hashes))
            .filter_map(|((i, bucket), (local_bucket_hash, remote_bucket_hash))| {
                (local_bucket_hash != remote_bucket_hash).then(|| {
                    let mut state = T::default();
                    state.join(bucket.into_values().collect());

                    (i, state)
                })
            })
            .collect::<HashMap<_, _>>();

        tracker.register(DefaultEvent::LocalToRemote {
            state: remote_unknown
                .iter()
                .chain(non_matching.values())
                .map(<T as Measure>::size_of)
                .sum(),
            metadata: non_matching.keys().count() * mem::size_of::<usize>(),
            upload: tracker.upload(),
        });

        let local_buckets = non_matching;
        let remote_buckets = remote_buckets
            .into_iter()
            .enumerate()
            .filter_map(|(i, bucket)| {
                local_buckets.contains_key(&i).then(|| {
                    let mut state = T::default();
                    state.join(bucket.into_values().collect());

                    (i, state)
                })
            })
            .collect::<HashMap<_, _>>();

        debug_assert_eq!(local_buckets.len(), remote_buckets.len());
        debug_assert!(remote_buckets.keys().all(|k| local_buckets.contains_key(k)));

        // 4. Compute the differences between buckets against both the local and remote
        //    decompositions. Then send the difference unknown by remote replica.
        //    NOTE: These step allows to filter any remaining false positives.
        let remote_false_positives = remote_buckets
            .iter()
            .map(|(i, remote)| local_buckets.get(i).unwrap().difference(remote));
        let local_false_positives = local_buckets
            .iter()
            .map(|(i, local)| remote_buckets.get(i).unwrap().difference(local))
            .collect::<Vec<_>>();

        tracker.register(DefaultEvent::RemoteToLocal {
            state: local_false_positives
                .iter()
                .map(<T as Measure>::size_of)
                .sum(),
            metadata: 0,
            download: tracker.download(),
        });

        // 5. Join the appropriate join-decompositions to each replica.
        self.remote.join(remote_unknown);
        self.remote.join(remote_false_positives.collect());

        self.local.join(local_unknown);
        self.local.join(local_false_positives);

        // 6. Sanity Check.
        tracker.finish(<T as Measure>::false_matches(&self.local, &self.remote));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{crdt::GSet, tracker::Bandwidth};

    #[test]
    fn test_sync() {
        let local = {
            let mut gset = GSet::new();
            let items = "a b c d e f g h i j k l"
                .split_whitespace()
                .collect::<Vec<_>>();

            for item in items {
                gset.insert(item.to_string());
            }

            gset
        };

        let remote = {
            let mut gset = GSet::new();
            let items = "m n o p q r s t u v w x y z"
                .split_whitespace()
                .collect::<Vec<_>>();

            for item in items {
                gset.insert(item.to_string());
            }

            gset
        };

        let buckets = local.len();
        let mut baseline = BloomBuckets::new(local, remote, 0.01, buckets);

        let (download, upload) = (Bandwidth::Kbps(0.5), Bandwidth::Kbps(0.5));
        let mut tracker = DefaultTracker::new(download, upload);

        baseline.sync(&mut tracker);
        assert_eq!(tracker.false_matches(), 0);

        let events = tracker.events();
        assert_eq!(events.len(), 4);
    }
}
