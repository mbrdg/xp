use std::{
    collections::HashMap, fmt::Display, hash::RandomState, iter::zip, marker::PhantomData, mem,
};

use crate::{
    crdt::{Decompose, Extract, Measure},
    tracker::{DefaultEvent, DefaultTracker, Telemetry},
};

use super::{Algorithm, BuildFilter, Dispatcher};

#[derive(Clone, Copy, Debug)]
pub struct BloomBuckets<T> {
    fpr: f64,
    lf: f64,
    _marker: PhantomData<T>,
}

impl<T> BloomBuckets<T> {
    #[inline]
    #[must_use]
    pub fn new(fpr: f64, lf: f64) -> Self {
        assert!(
            fpr > 0.0 && (0.0..1.0).contains(&fpr),
            "fpr should be a ratio in the interval (0.0, 1.0)"
        );
        assert!(lf > 0.0, "load factor should be greater than 0.0");

        Self {
            fpr,
            lf,
            _marker: PhantomData,
        }
    }
}

impl<T> Default for BloomBuckets<T> {
    fn default() -> Self {
        Self {
            fpr: 0.01,
            lf: 1.0,
            _marker: PhantomData,
        }
    }
}

impl<T> Display for BloomBuckets<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Bloom+Bucketing[fpr={}%,lf={}]",
            self.fpr * 100.0,
            self.lf
        )
    }
}

impl<T> BuildFilter<T> for BloomBuckets<T> where T: Extract {}
impl<T> Dispatcher<T> for BloomBuckets<T> where T: Clone + Decompose<Decomposition = T> + Extract {}

impl<T> Algorithm<T> for BloomBuckets<T>
where
    T: Clone + Decompose<Decomposition = T> + Default + Extract + Measure,
{
    type Tracker = DefaultTracker;

    fn sync(&self, local: &mut T, remote: &mut T, tracker: &mut Self::Tracker) {
        assert!(
            tracker.is_ready(),
            "tracker should be ready, i.e., no captured events and not finished"
        );

        let hasher = RandomState::new();
        let buckets = (self.lf * <T as Measure>::len(local) as f64) as usize;

        // 1. Create a filter from the local join-deocompositions and send it to the remote replica.
        let local_decompositions = local.split();
        let local_filter = self.filter_from(&local_decompositions, self.fpr);

        tracker.register(DefaultEvent::LocalToRemote {
            state: 0,
            metadata: <Self as BuildFilter<T>>::size_of(&local_filter),
            upload: tracker.upload(),
        });

        // 2. Partion the remote join-decompositions into *probably* present in both replicas or
        //    *definitely not* present in the local replica.
        let (remote_common, local_unknown) = self.partition(&local_filter, remote.split());

        // 3. Build a filter from the partion of *probably* common join-decompositions and send it
        //    to the local replica. At this stage the remote replica also constructs its buckets.
        //    For pipelining, the remaining decompositions and bucket's hashes are also sent.
        let remote_filter = self.filter_from(&remote_common, self.fpr);
        let remote_buckets = {
            let mut state = T::default();
            state.join(remote_common);

            self.dispatch(&state, buckets, &hasher)
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

            self.dispatch(&state, buckets, &hasher)
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
        remote.join(remote_unknown);
        remote.join(remote_false_positives.collect());

        local.join(local_unknown);
        local.join(local_false_positives);

        // 6. Sanity Check.
        tracker.finish(<T as Measure>::false_matches(local, remote));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{crdt::GSet, tracker::Bandwidth};

    #[test]
    fn test_sync() {
        let mut local = {
            let mut gset = GSet::new();
            let items = "a b c d e f g h i j k l"
                .split_whitespace()
                .collect::<Vec<_>>();

            for item in items {
                gset.insert(item.to_string());
            }

            gset
        };

        let mut remote = {
            let mut gset = GSet::new();
            let items = "m n o p q r s t u v w x y z"
                .split_whitespace()
                .collect::<Vec<_>>();

            for item in items {
                gset.insert(item.to_string());
            }

            gset
        };

        let (download, upload) = (Bandwidth::Kbps(0.5), Bandwidth::Kbps(0.5));
        let mut tracker = DefaultTracker::new(download, upload);
        let bloom_buckets = BloomBuckets::new(0.01, 1.0);

        bloom_buckets.sync(&mut local, &mut remote, &mut tracker);
        assert_eq!(tracker.false_matches(), 0);

        let events = tracker.events();
        assert_eq!(events.len(), 4);
    }
}
