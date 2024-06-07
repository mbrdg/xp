use crate::{
    crdt::{Decomposable, GSet},
    tracker::{DefaultTracker, NetworkEvent, Tracker},
};

use super::Protocol;

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

    fn size_of(replica: &Self::Replica) -> usize {
        replica.elements().iter().map(String::len).sum()
    }

    fn sync(&mut self, tracker: &mut Self::Tracker) {
        assert!(
            tracker.is_ready(),
            "tracker should be ready, i.e., no captured events and not finished"
        );

        // 1. Send the entire state from the local to the remote replica.
        let local_state = self.local.clone();

        tracker.register(NetworkEvent::local_to_remote(
            tracker.upload(),
            <Baseline as Protocol>::size_of(&local_state),
        ));

        // 2. Compute the optimal delta based on the remote replica state.
        let remote_unseen = local_state.difference(&self.remote);
        let local_unseen = self.remote.difference(&local_state);

        tracker.register(NetworkEvent::remote_to_local(
            tracker.download(),
            <Baseline as Protocol>::size_of(&local_unseen),
        ));

        // 3. Join the decompositions that are unknown to the remote replica.
        self.remote.join(vec![remote_unseen]);
        self.local.join(vec![local_unseen]);

        // 4. Sanity check.
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

        let mut baseline = Baseline::new(local, remote);
        let (download, upload) = (NetworkBandwitdth::Kbps(0.5), NetworkBandwitdth::Kbps(0.5));
        let mut tracker = DefaultTracker::new(download, upload);

        baseline.sync(&mut tracker);

        let bytes: Vec<_> = tracker.events().iter().map(NetworkEvent::bytes).collect();
        assert_eq!(bytes, vec![30, 35]);
        assert_eq!(tracker.diffs(), 0);
    }
}
