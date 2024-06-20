use std::marker::PhantomData;

use crate::{
    crdt::{Decompose, Extract, Measure},
    tracker::{DefaultEvent, DefaultTracker, Telemetry},
};

use super::{Algorithm, BuildFilter};

#[derive(Clone, Copy, Debug)]
pub struct Bloom<T> {
    fpr: f64,
    _marker: PhantomData<T>,
}

impl<T> Bloom<T> {
    #[inline]
    #[must_use]
    pub fn new(fpr: f64) -> Self {
        Self {
            fpr,
            _marker: PhantomData,
        }
    }
}

impl<T> BuildFilter<T> for Bloom<T> where T: Extract {}

impl<T> Algorithm<T> for Bloom<T>
where
    T: Decompose<Decomposition = T> + Extract + Measure,
{
    type Tracker = DefaultTracker;

    fn sync(&self, local: &mut T, remote: &mut T, tracker: &mut Self::Tracker) {
        assert!(
            tracker.is_ready(),
            "tracker should be ready, i.e., no captured events and not finished"
        );

        // 1. Create a filter from the local join-deocompositions and send it to the remote replica.
        let local_split = local.split();
        let local_filter = self.filter_from(&local_split, self.fpr);

        tracker.register(DefaultEvent::LocalToRemote {
            state: 0,
            metadata: <Self as BuildFilter<T>>::size_of(&local_filter),
            upload: tracker.upload(),
        });

        // 2. Partion the remote join-decompositions into *probably* present in both replicas or
        //    *definitely not* present in the local replica.
        let (common, local_unknown) = self.partition(&local_filter, remote.split());

        // 3. Build a filter from the partion of *probably* common join-decompositions and send it
        //    to the local replica. For pipelining, the remaining decompositions are also sent.
        let remote_filter = self.filter_from(&common, self.fpr);

        tracker.register(DefaultEvent::RemoteToLocal {
            state: local_unknown.iter().map(<T as Measure>::size_of).sum(),
            metadata: <Self as BuildFilter<T>>::size_of(&remote_filter),
            download: tracker.download(),
        });

        // 4. Do the same procedure as in 2., but this time in the local replica. This determines
        //    *not all* join-decompositions that are unknown by the remote replica.
        let remote_unknown = self.partition(&remote_filter, local_split).1;

        tracker.register(DefaultEvent::LocalToRemote {
            state: remote_unknown.iter().map(<T as Measure>::size_of).sum(),
            metadata: 0,
            upload: tracker.upload(),
        });

        // 5. Join the incoming join-decompositions on both replicas.
        local.join(local_unknown);
        remote.join(remote_unknown);

        // 6. Sanity check.
        // NOTE: This algorithm does not guarantee full sync.
        tracker.finish(<T as Measure>::false_matches(local, remote));
    }
}
