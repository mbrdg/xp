use std::{
    collections::HashMap, fmt::Display, hash::RandomState, iter::zip, marker::PhantomData, mem,
};

use crate::{
    crdt::{Decompose, Extract, Measure},
    tracker::{DefaultEvent, DefaultTracker, Telemetry},
};

use super::{Algorithm, Dispatcher};

#[derive(Clone, Copy, Debug)]
pub struct Buckets<T> {
    lf: f64,
    _marker: PhantomData<T>,
}

impl<T> Buckets<T> {
    #[inline]
    #[must_use]
    pub fn new(lf: f64) -> Self {
        assert!(lf > 0.0, "load factor should be greater than 0.0");

        Self {
            lf,
            _marker: PhantomData,
        }
    }
}

impl<T> Default for Buckets<T> {
    fn default() -> Self {
        Self {
            lf: 1.0,
            _marker: PhantomData,
        }
    }
}

impl<T> Display for Buckets<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Bucketing[lf={}]", self.lf)
    }
}

impl<T> Dispatcher<T> for Buckets<T> where T: Clone + Decompose<Decomposition = T> + Extract {}

impl<T> Algorithm<T> for Buckets<T>
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

        // 1. Assign each join-decomposition to a bucket based on the modulo of its hash and send
        //    the hashes to the remote replica.
        // NOTE: This policy must be deterministic across both peers.
        let local_buckets = self.dispatch(local, buckets, &hasher);
        let local_hashes = Buckets::<T>::hashes(&local_buckets, &hasher);

        tracker.register(DefaultEvent::LocalToRemote {
            state: 0,
            metadata: mem::size_of_val(local_hashes.as_slice()),
            upload: tracker.upload(),
        });

        // 2. Repeat the procedure from 1., but now on the remote replica.
        let remote_buckets = self.dispatch(remote, buckets, &hasher);
        let remote_hashes = Buckets::<T>::hashes(&remote_buckets, &hasher);

        // 3. Compute the buckets whose hash does not match on the remote replica and send those
        //    buckets back to the local replica.
        let non_matching = remote_buckets
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

        tracker.register(DefaultEvent::RemoteToLocal {
            state: non_matching.values().map(<T as Measure>::size_of).sum(),
            metadata: non_matching.keys().count() * mem::size_of::<usize>(),
            download: tracker.download(),
        });

        // 4. Compute the differences between buckets against both the local and remote
        //    decompositions. Then send the difference unknown by remote replica.
        let remote_buckets = non_matching;
        let local_buckets = local_buckets
            .into_iter()
            .enumerate()
            .filter_map(|(i, bucket)| {
                remote_buckets.contains_key(&i).then(|| {
                    let mut state = T::default();
                    state.join(bucket.into_values().collect());

                    (i, state)
                })
            })
            .collect::<HashMap<_, _>>();

        let local_unknown = local_buckets
            .iter()
            .map(|(i, local)| remote_buckets.get(i).unwrap().difference(local));
        let remote_unknown = remote_buckets
            .iter()
            .map(|(i, remote)| local_buckets.get(i).unwrap().difference(remote))
            .collect::<Vec<_>>();

        tracker.register(DefaultEvent::LocalToRemote {
            state: remote_unknown.iter().map(<T as Measure>::size_of).sum(),
            metadata: 0,
            upload: tracker.upload(),
        });

        // 5. Join the appropriate join-decompositions to each replica.
        local.join(local_unknown.collect());
        remote.join(remote_unknown);

        // 6. Sanity check.
        tracker.finish(<T as Measure>::false_matches(local, remote));
    }
}

#[cfg(test)]
mod tests {
    use std::mem;

    use super::*;
    use crate::{crdt::GSet, tracker::Bandwidth};

    #[test]
    fn test_sync() {
        let mut local = {
            let mut gset = GSet::new();
            let items = "Stuck In A Moment You Can't Get Out Of"
                .split_whitespace()
                .collect::<Vec<_>>();

            for item in items {
                gset.insert(item.to_string());
            }

            gset
        };

        let mut remote = {
            let mut gset = GSet::new();
            let items = "I Still Haven't Found What I'm Looking For"
                .split_whitespace()
                .collect::<Vec<_>>();

            for item in items {
                gset.insert(item.to_string());
            }

            gset
        };

        let (download, upload) = (Bandwidth::Kbps(0.5), Bandwidth::Kbps(0.5));
        let mut tracker = DefaultTracker::new(download, upload);
        let buckets = Buckets::new(1.25);

        buckets.sync(&mut local, &mut remote, &mut tracker);

        let bytes: Vec<_> = tracker.events().iter().map(DefaultEvent::bytes).collect();
        assert_eq!(bytes[0], 11 * mem::size_of::<u64>());
        assert_eq!(tracker.false_matches(), 0);
    }
}
