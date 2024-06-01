#![allow(dead_code)]

use std::{
    any,
    time::{Duration, Instant},
};

use crate::{
    crdt::gset::GSet,
    sync::{
        baseline::Baseline, bloombuckets::BloomBuckets, buckets::Buckets, BuildProtocol, Protocol,
    },
    tracker::{DefaultTracker, NetworkBandwitdth, NetworkEvent, Tracker},
};

use rand::{
    distributions::{Alphanumeric, DistString},
    random,
    rngs::StdRng,
    SeedableRng,
};

mod bloom;
mod crdt;
mod sync;
mod tracker;

/// Populates replicas with random data given a similarity degree.
fn populate(
    num_items: usize,
    item_size: usize,
    similarity: f64,
    rng: &mut StdRng,
) -> (GSet<String>, GSet<String>) {
    assert!(
        (0.0..=1.0).contains(&similarity),
        "similarity should be a ratio between 0.0 and 1.0"
    );

    let similar = (num_items as f64 * similarity) as usize;
    let diffs = num_items - similar;

    let common = (0..similar)
        .map(|_| Alphanumeric.sample_string(rng, item_size))
        .collect::<Vec<_>>();

    let local = {
        let mut gset = GSet::new();
        let specific = (0..diffs).map(|_| Alphanumeric.sample_string(rng, item_size));
        let items = common.iter().cloned().chain(specific);

        for item in items {
            gset.insert(item);
        }

        gset
    };

    let remote = {
        let mut gset = GSet::new();
        let specific = (0..diffs).map(|_| Alphanumeric.sample_string(rng, item_size));
        let items = common.into_iter().chain(specific);

        for item in items {
            gset.insert(item);
        }

        gset
    };

    debug_assert_eq!(
        2 * diffs,
        local
            .elements()
            .symmetric_difference(remote.elements())
            .count(),
    );

    (local, remote)
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

    let mut rng = StdRng::seed_from_u64(seed);

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
            let (local, remote) = populate(num_items, item_size, s, &mut rng);

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
