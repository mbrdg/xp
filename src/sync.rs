use crate::crdt::{Decomposable, GSet};

#[derive(Debug, Default)]
pub struct Metrics {
    bytes_exchanged: usize,
    round_trips: u8,
    false_matches: usize,
}

pub trait Algorithm<R> {
    fn sync(&mut self) -> Metrics;
    fn sizeof(replica: &R) -> usize;
}

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

impl Algorithm<GSet<String>> for Baseline {
    fn sync(&mut self) -> Metrics {
        let mut metrics = Metrics::default();

        // 1. Ship the full local state and send them to the remote peer
        let local_state = self.local.clone();
        metrics.bytes_exchanged += Baseline::sizeof(&local_state);
        metrics.round_trips += 1;

        // 2. The remote peer computes the optimal delta from its current state
        let remote_unseen = local_state.difference(&self.remote);
        let local_unseen = self.remote.difference(&local_state);

        self.remote.join(vec![remote_unseen]);

        metrics.bytes_exchanged += Baseline::sizeof(&local_unseen);
        metrics.round_trips += 1;

        // 3. Merge the minimum delta from the remote peer
        self.local.join(vec![local_unseen]);

        // 4. sanity check, i.e., false matches must be 0
        metrics.false_matches = self
            .local
            .elements()
            .symmetric_difference(self.remote.elements())
            .count();

        metrics
    }

    fn sizeof(replica: &GSet<String>) -> usize {
        replica.elements().iter().map(String::len).sum()
    }
}
