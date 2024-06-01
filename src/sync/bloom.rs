use crate::{
    bloom::BloomFilter,
    crdt::{gset::GSet, Decomposable},
    tracker::{DefaultTracker, NetworkEvent, Tracker},
};

use super::{BuildBloomFilters, BuildProtocol, Protocol, ReplicaSize};

pub struct Bloom {
    local: GSet<String>,
    remote: GSet<String>,
    fpr: f64,
}

pub struct BloomBuilder {
    local: GSet<String>,
    remote: GSet<String>,
    fpr: Option<f64>,
}

impl BloomBuilder {
    pub fn fpr(mut self, fpr: f64) -> Self {
        assert!(
            (0.0..1.0).contains(&fpr) && fpr > 0.0,
            "false positive rate should be a ratio greater than 0.0"
        );

        self.fpr = Some(fpr);
        self
    }
}

impl BuildProtocol for BloomBuilder {
    type Protocol = Bloom;

    fn build(self) -> Self::Protocol {
        Bloom {
            local: self.local,
            remote: self.remote,
            fpr: self.fpr.unwrap_or(0.01),
        }
    }
}

impl BuildBloomFilters for Bloom {
    type Decomposition = GSet<String>;
    type Item = String;

    fn filter(decompositions: &[Self::Decomposition], fpr: f64) -> BloomFilter<Self::Item> {
        let mut filter = BloomFilter::new(decompositions.len(), fpr);
        decompositions.iter().for_each(|delta| {
            let item = delta
                .elements()
                .iter()
                .next()
                .expect("a decomposition should have a single item");
            filter.insert(item);
        });

        filter
    }
}

impl ReplicaSize for Bloom {
    type Replica = GSet<String>;

    fn size_of(replica: &Self::Replica) -> usize {
        replica.elements().iter().map(String::len).sum()
    }
}

impl Protocol for Bloom {
    type Replica = GSet<String>;
    type Builder = BloomBuilder;
    type Tracker = DefaultTracker;

    fn builder(local: Self::Replica, remote: Self::Replica) -> Self::Builder {
        BloomBuilder {
            local,
            remote,
            fpr: None,
        }
    }

    fn sync(&mut self, tracker: &mut Self::Tracker) {
        assert!(
            tracker.is_ready(),
            "tracker should be ready, i.e., no captured events and not finished"
        );

        // 1.1. Split the local state and insert each decomposition into a Bloom Filter.
        let local_split = self.local.split();
        let local_filter = Bloom::filter(&local_split, self.fpr);

        // 1.2. Ship the Bloom filter to the remote replica.
        tracker.register(NetworkEvent::local_to_remote(
            tracker.upload(),
            <Bloom as BuildBloomFilters>::size_of(&local_filter),
        ));

        // 2.1. At the remote replica, split the state into join decompositions. Then split the
        //   decompositions into common, i.e., present in both replicas, and unknown, i.e., present
        //   remotely but not locally.
        let (common, local_unkown): (Vec<_>, Vec<_>) =
            self.remote.split().into_iter().partition(|delta| {
                let item = delta
                    .elements()
                    .iter()
                    .next()
                    .expect("a decomposition should have a single item");
                local_filter.contains(item)
            });

        // 2.2. From the common partions build a Bloom Filter.
        let remote_filter = Bloom::filter(common.as_slice(), self.fpr);

        // 2.3. Send back to the remote replica the unknown decompositions and the Bloom Filter.
        tracker.register(NetworkEvent::remote_to_local(
            tracker.download(),
            <Bloom as BuildBloomFilters>::size_of(&remote_filter)
                + local_unkown
                    .iter()
                    .map(<Bloom as ReplicaSize>::size_of)
                    .sum::<usize>(),
        ));

        // 3.1. At the local replica, split the state into join decompositions. Then split the
        //   decompositions into common, i.e., present in both replicas, and unknown, i.e., present
        //   locally but not remotely.
        let remote_unknown: Vec<_> = local_split
            .into_iter()
            .filter(|delta| {
                let item = delta
                    .elements()
                    .iter()
                    .next()
                    .expect("a decomposition should have a single item");
                !remote_filter.contains(item)
            })
            .collect();

        // 3.2. Join the incoming local unknown decompositons.
        self.local.join(local_unkown);

        // 3.3. Send to the remote replica the unkown decompositions.
        tracker.register(NetworkEvent::local_to_remote(
            tracker.upload(),
            remote_unknown
                .iter()
                .map(<Bloom as ReplicaSize>::size_of)
                .sum(),
        ));

        // 4. Join the incoming remote unkown decompositions.
        self.remote.join(remote_unknown);

        // 5. Sanity check.
        // WARN: This algorithm does not guarantee full state sync between replicas.
        tracker.finish(
            self.local
                .elements()
                .symmetric_difference(self.remote.elements())
                .count(),
        );
    }
}
