use crate::{
    crdt::{gset::GSet, Decomposable},
    tracker::{DefaultTracker, NetworkEvent, Tracker},
};

use super::{BuildProtocol, Protocol, ReplicaSize};

pub struct Baseline {
    local: GSet<String>,
    remote: GSet<String>,
}

pub struct BaselineBuilder {
    local: GSet<String>,
    remote: GSet<String>,
}

impl BuildProtocol for BaselineBuilder {
    type Protocol = Baseline;

    fn build(self) -> Self::Protocol {
        Baseline {
            local: self.local,
            remote: self.remote,
        }
    }
}

impl ReplicaSize for Baseline {
    type Replica = GSet<String>;

    fn size_of(replica: &Self::Replica) -> usize {
        replica.elements().iter().map(String::len).sum()
    }
}

impl Protocol for Baseline {
    type Replica = GSet<String>;
    type Builder = BaselineBuilder;
    type Tracker = DefaultTracker;

    fn builder(local: Self::Replica, remote: Self::Replica) -> Self::Builder {
        BaselineBuilder { local, remote }
    }

    fn sync(&mut self, tracker: &mut Self::Tracker) {
        assert!(
            tracker.is_ready(),
            "tracker should be ready, i.e., no captured events and not finished"
        );

        // 1. Ship the full local state and send it the remote replica.
        let local_state = self.local.clone();

        tracker.register(NetworkEvent::local_to_remote(
            tracker.upload(),
            Baseline::size_of(&local_state),
        ));

        // 2.1. Compute the optimal delta based on the remote replica state.
        let remote_unseen = local_state.difference(&self.remote);
        let local_unseen = self.remote.difference(&local_state);

        // 2.2. Join the decompositions that are unknown to the remote replica.
        self.remote.join(vec![remote_unseen]);

        // 2.3. Send back to the local replica the decompositions unknown to the local replica.
        tracker.register(NetworkEvent::remote_to_local(
            tracker.download(),
            Baseline::size_of(&local_unseen),
        ));

        // 3. Merge the minimum delta received from the remote replica.
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

        let mut baseline = Baseline::builder(local, remote).build();
        let (download, upload) = (NetworkBandwitdth::Kbps(0.5), NetworkBandwitdth::Kbps(0.5));
        let mut tracker = DefaultTracker::new(download, upload);

        baseline.sync(&mut tracker);

        let bytes: Vec<_> = tracker.events().iter().map(NetworkEvent::bytes).collect();
        assert_eq!(bytes, vec![30, 35]);
        assert_eq!(tracker.diffs(), 0);
    }
}
