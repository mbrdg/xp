use std::{collections::HashMap, iter::zip, mem};

use crate::{
    crdt::{Decomposable, Measurable},
    tracker::{DefaultTracker, NetworkEvent, Tracker},
};

use super::{BucketDispatcher, Protocol};

pub struct Buckets<T> {
    dispatcher: BucketDispatcher<T>,
    local: T,
    remote: T,
}

impl<T> Buckets<T> {
    #[inline]
    #[must_use]
    pub fn new(local: T, remote: T, dispatcher: BucketDispatcher<T>) -> Self {
        Self {
            local,
            remote,
            dispatcher,
        }
    }
}

impl<T> Protocol for Buckets<T>
where
    T: Clone + Decomposable<Decomposition = T> + Default + Measurable,
{
    type Tracker = DefaultTracker;

    fn sync(&mut self, tracker: &mut Self::Tracker) {
        assert!(
            tracker.is_ready(),
            "tracker should be ready, i.e., no captured events and not finished"
        );

        // 1. Assign each join-decomposition to a bucket based on the modulo of its hash and send
        //    the hashes to the remote replica.
        // NOTE: This policy must be deterministic across both peers.
        let local_buckets = self.dispatcher.dispatch(&self.local);
        let local_hashes = self.dispatcher.hashes(&local_buckets);

        tracker.register(NetworkEvent::local_to_remote(
            tracker.upload(),
            mem::size_of_val(local_hashes.as_slice()),
        ));

        // 2. Repeat the procedure from 1., but now on the remote replica.
        let remote_buckets = self.dispatcher.dispatch(&self.remote);
        let remote_hashes = self.dispatcher.hashes(&remote_buckets);

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

        tracker.register(NetworkEvent::remote_to_local(
            tracker.download(),
            non_matching
                .iter()
                .map(|(i, r)| mem::size_of_val(i) + <T as Measurable>::size_of(r))
                .sum(),
        ));

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

        tracker.register(NetworkEvent::local_to_remote(
            tracker.upload(),
            remote_unknown.iter().map(<T as Measurable>::size_of).sum(),
        ));

        // 5. Join the appropriate join-decompositions to each replica.
        self.local.join(local_unknown.collect());
        self.remote.join(remote_unknown);

        // 6. Sanity check.
        tracker.finish(<T as Measurable>::false_matches(&self.local, &self.remote));
    }
}

#[cfg(test)]
mod tests {
    use std::mem;

    use super::*;
    use crate::{crdt::GSet, tracker::NetworkBandwitdth};

    #[test]
    fn test_sync() {
        let local = {
            let mut gset = GSet::new();
            let items = "Stuck In A Moment You Can't Get Out Of"
                .split_whitespace()
                .collect::<Vec<_>>();

            for item in items {
                gset.insert(item.to_string());
            }

            gset
        };

        let remote = {
            let mut gset = GSet::new();
            let items = "I Still Haven't Found What I'm Looking For"
                .split_whitespace()
                .collect::<Vec<_>>();

            for item in items {
                gset.insert(item.to_string());
            }

            gset
        };

        let dispatcher = BucketDispatcher::new((1.25 * local.len() as f64) as usize);
        let mut baseline = Buckets::new(local, remote, dispatcher);

        let (download, upload) = (NetworkBandwitdth::Kbps(0.5), NetworkBandwitdth::Kbps(0.5));
        let mut tracker = DefaultTracker::new(download, upload);

        baseline.sync(&mut tracker);

        let bytes: Vec<_> = tracker.events().iter().map(NetworkEvent::bytes).collect();
        assert_eq!(bytes[0], 11 * mem::size_of::<u64>());
        assert_eq!(tracker.diffs(), 0);
    }
}
