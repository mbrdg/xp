use std::{fmt::Display, marker::PhantomData};

use crate::{
    crdt::{Decompose, Measure},
    tracker::{DefaultEvent, DefaultTracker, Telemetry},
};

use super::Algorithm;

#[derive(Clone, Copy, Debug, Default)]
pub struct Baseline<T> {
    _marker: PhantomData<T>,
}

impl<T> Baseline<T> {
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T> Display for Baseline<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Baseline")
    }
}

impl<T> Algorithm<T> for Baseline<T>
where
    T: Clone + Decompose<Decomposition = T> + Measure,
{
    type Tracker = DefaultTracker;

    fn sync(&self, local: &mut T, remote: &mut T, tracker: &mut Self::Tracker) {
        assert!(
            tracker.is_ready(),
            "tracker should be ready, i.e., no captured events and not finished"
        );

        // 1. Send the entire state from the local to the remote replica.
        let local_state = local.clone();

        tracker.register(DefaultEvent::LocalToRemote {
            state: <T as Measure>::size_of(&local_state),
            metadata: 0,
            upload: tracker.upload(),
        });

        // 2. Compute the optimal delta based on the remote replica state.
        let remote_unseen = local_state.difference(remote);
        let local_unseen = remote.difference(&local_state);

        tracker.register(DefaultEvent::RemoteToLocal {
            state: <T as Measure>::size_of(&local_unseen),
            metadata: 0,
            download: tracker.download(),
        });

        // 3. Join the decompositions that are unknown to the remote replica.
        remote.join(vec![remote_unseen]);
        local.join(vec![local_unseen]);

        // 4. Sanity check.
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
            let mut gset = GSet::<String>::new();
            let items = "Stuck In A Moment You Can't Get Out Of"
                .split_whitespace()
                .collect::<Vec<_>>();

            for item in items {
                gset.insert(item.to_string());
            }

            gset
        };

        let mut remote = {
            let mut gset = GSet::<String>::new();
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

        let baseline = Baseline::new();
        baseline.sync(&mut local, &mut remote, &mut tracker);

        let bytes: Vec<_> = tracker.events().iter().map(DefaultEvent::bytes).collect();
        assert_eq!(bytes, vec![30, 35]);
        assert_eq!(tracker.false_matches(), 0);
    }
}
