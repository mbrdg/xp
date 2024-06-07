use std::{collections::HashMap, iter::zip, mem};

use crate::{
    crdt::{Decomposable, GSet},
    tracker::{DefaultTracker, NetworkEvent, Tracker},
};

use super::{Bloomer, BucketDispatcher, Protocol};

pub struct BloomBuckets {
    bloomer: Bloomer<GSet<String>>,
    dispatcher: BucketDispatcher<GSet<String>>,
    local: GSet<String>,
    remote: GSet<String>,
}

impl BloomBuckets {
    #[inline]
    #[must_use]
    pub fn new(local: GSet<String>, remote: GSet<String>) -> Self {
        let num_buckets = (1.01 * local.len() as f64) as usize;

        Self {
            bloomer: Bloomer::new(0.01),
            dispatcher: BucketDispatcher::new(num_buckets),
            local,
            remote,
        }
    }

    #[inline]
    #[must_use]
    pub fn with_bloomer(
        local: GSet<String>,
        remote: GSet<String>,
        bloomer: Bloomer<GSet<String>>,
    ) -> Self {
        let num_buckets = (1.0 + bloomer.fpr() * local.len() as f64) as usize;

        Self {
            bloomer,
            dispatcher: BucketDispatcher::new(num_buckets),
            local,
            remote,
        }
    }
}

impl Protocol for BloomBuckets {
    type Replica = GSet<String>;
    type Tracker = DefaultTracker;

    fn size_of(replica: &Self::Replica) -> usize {
        replica.elements().iter().map(String::len).sum()
    }

    fn sync(&mut self, tracker: &mut Self::Tracker) {
        assert!(
            tracker.is_ready(),
            "tracker should be ready, i.e., no captured events and not finished"
        );

        // 1. Create a filter from the local join-deocompositions and send it to the remote replica.
        let local_decompositions = self.local.split();
        let local_filter = self.bloomer.filter_from(&local_decompositions);

        tracker.register(NetworkEvent::local_to_remote(
            tracker.upload(),
            Bloomer::size_of(&local_filter),
        ));

        // 2. Partion the remote join-decompositions into *probably* present in both replicas or
        //    *definitely not* present in the local replica.
        let (remote_common, local_unknown) =
            self.bloomer.partition(&local_filter, self.remote.split());

        // 3. Build a filter from the partion of *probably* common join-decompositions and send it
        //    to the local replica. At this stage the remote replica also constructs its buckets.
        //    For pipelining, the remaining decompositions and bucket's hashes are also sent.
        let remote_filter = self.bloomer.filter_from(&remote_common);
        let remote_buckets = {
            let mut state = GSet::new();
            state.join(remote_common);

            self.dispatcher.dispatch(&state)
        };
        let remote_hashes = self.dispatcher.hashes(&remote_buckets);

        tracker.register(NetworkEvent::remote_to_local(
            tracker.download(),
            local_unknown
                .iter()
                .map(<BloomBuckets as Protocol>::size_of)
                .sum::<usize>()
                + Bloomer::size_of(&remote_filter),
        ));

        // 3. Compute the buckets whose hash does not match on the local replica and send those
        //    buckets back to the remote replica together with all the decompositions that are
        //    *definitely not* on the remote replica.
        let (local_common, remote_unknown) =
            self.bloomer.partition(&remote_filter, local_decompositions);

        // Assign each join-decomposition from the set of *probably* common join-decompositions to
        // a bucket based on the modulo of its hash and send the hashes to the remote replica.
        // NOTE: This policy must be deterministic across both peers.
        let local_buckets = {
            let mut state = GSet::new();
            state.join(local_common);

            self.dispatcher.dispatch(&state)
        };
        let local_hashes = self.dispatcher.hashes(&local_buckets);

        let non_matching = local_buckets
            .into_iter()
            .enumerate()
            .zip(zip(local_hashes, remote_hashes))
            .filter_map(|((i, bucket), (local_bucket_hash, remote_bucket_hash))| {
                (local_bucket_hash != remote_bucket_hash).then(|| {
                    let mut state = GSet::new();
                    state.join(bucket.into_values().collect());

                    (i, state)
                })
            })
            .collect::<HashMap<_, _>>();

        tracker.register(NetworkEvent::local_to_remote(
            tracker.upload(),
            remote_unknown
                .iter()
                .map(<BloomBuckets as Protocol>::size_of)
                .sum::<usize>()
                + non_matching
                    .iter()
                    .map(|(i, r)| mem::size_of_val(i) + <BloomBuckets as Protocol>::size_of(r))
                    .sum::<usize>(),
        ));

        let local_buckets = non_matching;
        let remote_buckets = remote_buckets
            .into_iter()
            .enumerate()
            .filter_map(|(i, bucket)| {
                local_buckets.contains_key(&i).then(|| {
                    let mut state = GSet::new();
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

        tracker.register(NetworkEvent::remote_to_local(
            tracker.download(),
            local_false_positives
                .iter()
                .map(<BloomBuckets as Protocol>::size_of)
                .sum(),
        ));

        // 5. Join the appropriate join-decompositions to each replica.
        self.remote.join(remote_unknown);
        self.remote.join(remote_false_positives.collect());

        self.local.join(local_unknown);
        self.local.join(local_false_positives);

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
            let items = "a b c d e f g h i j k l"
                .split_whitespace()
                .collect::<Vec<_>>();

            for item in items {
                gset.insert(item.to_string());
            }

            gset
        };

        let remote = {
            let mut gset = GSet::<String>::new();
            let items = "m n o p q r s t u v w x y z"
                .split_whitespace()
                .collect::<Vec<_>>();

            for item in items {
                gset.insert(item.to_string());
            }

            gset
        };

        let mut baseline = BloomBuckets::new(local, remote);
        let (download, upload) = (NetworkBandwitdth::Kbps(0.5), NetworkBandwitdth::Kbps(0.5));
        let mut tracker = DefaultTracker::new(download, upload);

        baseline.sync(&mut tracker);
        assert_eq!(tracker.diffs(), 0);

        let events = tracker.events();
        assert_eq!(events.len(), 4);
    }
}
