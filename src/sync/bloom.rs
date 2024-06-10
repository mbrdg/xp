use crate::{
    crdt::{Decomposable, Measurable},
    tracker::{DefaultTracker, NetworkEvent, Tracker},
};

use super::{Bloomer, Protocol};

pub struct Bloom<T> {
    bloomer: Bloomer<T>,
    local: T,
    remote: T,
}

impl<T> Bloom<T> {
    #[inline]
    #[must_use]
    pub fn new(local: T, remote: T) -> Self {
        Self {
            bloomer: Bloomer::new(0.01),
            local,
            remote,
        }
    }

    #[inline]
    #[must_use]
    pub fn with_bloomer(local: T, remote: T, bloomer: Bloomer<T>) -> Self {
        Self {
            local,
            remote,
            bloomer,
        }
    }
}

impl<T> Protocol for Bloom<T>
where
    T: Clone + Decomposable<Decomposition = T> + Measurable,
{
    type Tracker = DefaultTracker;

    fn sync(&mut self, tracker: &mut Self::Tracker) {
        assert!(
            tracker.is_ready(),
            "tracker should be ready, i.e., no captured events and not finished"
        );

        // 1. Create a filter from the local join-deocompositions and send it to the remote replica.
        let local_split = self.local.split();
        let local_filter = self.bloomer.filter_from(&local_split);

        tracker.register(NetworkEvent::local_to_remote(
            tracker.upload(),
            Bloomer::size_of(&local_filter),
        ));

        // 2. Partion the remote join-decompositions into *probably* present in both replicas or
        //    *definitely not* present in the local replica.
        let (common, local_unknown) = self.bloomer.partition(&local_filter, self.remote.split());

        // 3. Build a filter from the partion of *probably* common join-decompositions and send it
        //    to the local replica. For pipelining, the remaining decompositions are also sent.
        let remote_filter = self.bloomer.filter_from(&common);

        tracker.register(NetworkEvent::remote_to_local(
            tracker.download(),
            Bloomer::size_of(&remote_filter)
                + local_unknown
                    .iter()
                    .map(<T as Measurable>::size_of)
                    .sum::<usize>(),
        ));

        // 4. Do the same procedure as in 2., but this time in the local replica. This determines
        //    *not all* join-decompositions that are unknown by the remote replica.
        let remote_unknown = self.bloomer.partition(&remote_filter, local_split).1;

        tracker.register(NetworkEvent::local_to_remote(
            tracker.upload(),
            remote_unknown.iter().map(<T as Measurable>::size_of).sum(),
        ));

        // 5. Join the incoming join-decompositions on both replicas.
        self.local.join(local_unknown);
        self.remote.join(remote_unknown);

        // 6. Sanity check.
        // NOTE: This algorithm does not guarantee full sync.
        tracker.finish(<T as Measurable>::false_matches(&self.local, &self.remote));
    }
}
