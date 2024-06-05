#![allow(dead_code)]

use std::{
    any,
    ops::Range,
    time::{Duration, Instant},
    usize,
};

use crate::{
    crdt::GSet,
    sync::{
        baseline::Baseline, bloombuckets::BloomBuckets, buckets::Buckets, BuildProtocol, Protocol,
    },
    tracker::{DefaultTracker, NetworkBandwitdth, NetworkEvent, Tracker},
};

use rand::{
    distributions::{Alphanumeric, DistString, Distribution, Uniform},
    random,
    rngs::StdRng,
    SeedableRng,
};

mod bloom;
mod crdt;
mod sync;
mod tracker;

struct ReplicaGenerator {
    rng: StdRng,
}

impl ReplicaGenerator {
    #[inline]
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
        }
    }

    pub fn generate_single(&mut self, cardinality: usize, lengths: Range<usize>) -> GSet<String> {
        let lengths = Uniform::from(lengths)
            .sample_iter(&mut self.rng)
            .take(cardinality)
            .collect::<Vec<_>>();

        let mut replica = GSet::new();
        lengths
            .into_iter()
            .map(|len| Alphanumeric.sample_string(&mut self.rng, len))
            .for_each(|item| {
                replica.insert(item);
            });

        replica
    }

    pub fn generate_pair_with_similarity(
        &mut self,
        cardinality: usize,
        lengths: Range<usize>,
        similarity: f64,
    ) -> (GSet<String>, GSet<String>) {
        assert!(
            (0.0..=1.0).contains(&similarity),
            "similarity should be a ratio between 0.0 and 1.0"
        );

        let common = (cardinality as f64 * similarity) as usize;
        let distinct = cardinality - common;

        let lengths = Uniform::from(lengths);

        let common_items = lengths
            .sample_iter(&mut self.rng)
            .take(common)
            .collect::<Vec<_>>()
            .into_iter()
            .map(|len| Alphanumeric.sample_string(&mut self.rng, len))
            .collect::<Vec<_>>();

        let local_only_items_iter = lengths
            .sample_iter(&mut self.rng)
            .take(distinct)
            .collect::<Vec<_>>()
            .into_iter()
            .map(|len| Alphanumeric.sample_string(&mut self.rng, len));

        let local_items = common_items.iter().cloned().chain(local_only_items_iter);
        let mut local = GSet::new();
        local_items.for_each(|item| {
            local.insert(item);
        });

        let remote_only_items_iter = lengths
            .sample_iter(&mut self.rng)
            .take(distinct)
            .collect::<Vec<_>>()
            .into_iter()
            .map(|len| Alphanumeric.sample_string(&mut self.rng, len));

        let remote_items = common_items.into_iter().chain(remote_only_items_iter);
        let mut remote = GSet::new();
        remote_items.for_each(|item| {
            remote.insert(item);
        });

        assert_eq!(
            2 * distinct,
            local
                .elements()
                .symmetric_difference(remote.elements())
                .count()
        );

        (local, remote)
    }
}

/// Runs the specified protocol and outputs the metrics obtained.
fn run<P>(
    protocol: &mut P,
    id: Option<&str>,
    similarity: f64,
    download: NetworkBandwitdth,
    upload: NetworkBandwitdth,
) where
    P: Protocol<Tracker = DefaultTracker>,
{
    assert!(
        (0.0..=1.0).contains(&similarity),
        "similarity should be a ratio between 0.0 and 1.0"
    );

    let mut tracker = DefaultTracker::new(download, upload);
    protocol.sync(&mut tracker);

    let type_name = {
        let name = any::type_name_of_val(&protocol).split("::").last().unwrap();
        id.map_or(name.to_string(), |i| format!("{name}<{i}>"))
    };

    let diffs = tracker.diffs();
    if diffs > 0 {
        eprintln!("{type_name} not totally synced with {diffs} diffs");
    }

    let events = tracker.events();

    let hops = events.len();
    let duration = events.iter().map(NetworkEvent::duration).sum::<Duration>();
    let bytes = events.iter().map(NetworkEvent::bytes).sum::<usize>();

    println!(
        "{type_name} {:.2} {hops} {:.3} {bytes}",
        similarity * 100.0,
        duration.as_secs_f64()
    );
}

fn main() {
    let execution_time = Instant::now();

    let (iters, seed) = (10, random());
    println!("{iters} {seed}");

    let mut gen = ReplicaGenerator::new(seed);

    let (num_items, item_size) = (25_000, 50);
    let (download, upload) = (NetworkBandwitdth::Kbps(32.0), NetworkBandwitdth::Kbps(32.0));
    println!(
        "{num_items} {item_size} {:.2} {:.2}",
        download.as_bytes_per_sec(),
        upload.as_bytes_per_sec()
    );

    // Varying similarity
    let (start, end, step) = (0, 100, 10);
    println!("{start} {end} {step}");

    let similarity = (start..=end)
        .rev()
        .step_by(step)
        .map(|s| f64::from(s) / 100.0);

    for s in similarity {
        for _ in 0..iters {
            let (local, remote) = gen.generate_pair_with_similarity(32_000, 50..81, s);

            let mut baseline = Baseline::builder(local.clone(), remote.clone()).build();
            run(&mut baseline, None, s, download, upload);

            let mut buckets = Buckets::builder(local.clone(), remote.clone())
                .load_factor(1.0)
                .build();
            run(&mut buckets, Some("1.0"), s, download, upload);

            let mut buckets = Buckets::builder(local.clone(), remote.clone())
                .load_factor(4.0)
                .build();
            run(&mut buckets, Some("4.0"), s, download, upload);

            let mut bloombuckets = BloomBuckets::builder(local, remote)
                .load_factor(1.0)
                .fpr(0.02)
                .build();
            run(&mut bloombuckets, Some("1.0, 0.02"), s, download, upload);
        }
    }

    eprintln!("time elapsed: {:.3?}", execution_time.elapsed());
}
