#![allow(dead_code)]

use std::{
    any,
    ops::Range,
    time::{Duration, Instant},
    usize,
};

use crate::{
    crdt::GSet,
    sync::Protocol,
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

fn populate_gset(cardinality: usize, lengths: Range<usize>, rng: &mut StdRng) -> GSet<String> {
    let mut replica = GSet::new();

    Uniform::from(lengths)
        .sample_iter(rng.clone())
        .map(|len| Alphanumeric.sample_string(rng, len))
        .take(cardinality)
        .for_each(|item| {
            replica.insert(item);
        });

    assert_eq!(cardinality, replica.len());
    replica
}

fn populate_similar_gsets(
    cardinality: usize,
    lengths: Range<usize>,
    similarity: f64,
    rng: &mut StdRng,
) -> (GSet<String>, GSet<String>) {
    assert!(
        (0.0..1.0).contains(&similarity),
        "similarity should be a ratio between 0.0 and 1.0"
    );

    let num_similar_items = (cardinality as f64 * similarity) as usize;
    let num_diff_items = cardinality - num_similar_items;

    let mut gsets = (GSet::new(), GSet::new());

    // Generate common items for both gsets
    Uniform::from(lengths.clone())
        .sample_iter(rng.clone())
        .map(|len| Alphanumeric.sample_string(rng, len))
        .take(num_similar_items)
        .for_each(|item| {
            gsets.0.insert(item.clone());
            gsets.1.insert(item);
        });

    // Generate diff items for the first gset
    Uniform::from(lengths.clone())
        .sample_iter(rng.clone())
        .map(|len| Alphanumeric.sample_string(rng, len))
        .take(num_diff_items)
        .for_each(|item| {
            gsets.0.insert(item);
        });

    // Generate diff items for the second gset
    Uniform::from(lengths)
        .sample_iter(rng.clone())
        .map(|len| Alphanumeric.sample_string(rng, len))
        .take(num_diff_items)
        .for_each(|item| {
            gsets.1.insert(item);
        });

    // Ensure that each set has exactly the appropriate number of distinct items
    assert_eq!(cardinality, gsets.0.len());
    assert_eq!(cardinality, gsets.1.len());
    assert_eq!(
        gsets
            .0
            .elements()
            .symmetric_difference(gsets.1.elements())
            .count(),
        2 * num_diff_items
    );

    gsets
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
