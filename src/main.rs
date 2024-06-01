#![allow(dead_code)]

use std::{
    any,
    time::{Duration, Instant},
};

use crate::{
    crdt::gset::GSet,
    sync::{baseline::Baseline, BuildProtocol, Protocol},
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
    count: usize,
    size: usize,
    similarity: usize,
    seed: u64,
) -> (GSet<String>, GSet<String>) {
    let similar_items = (count as f64 * (similarity as f64 / 100.0)) as usize;
    let diff_items = count - similar_items;

    let mut rng = StdRng::seed_from_u64(seed);

    let mut local = GSet::new();
    let mut remote = GSet::new();

    for _ in 0..similar_items {
        let item = Alphanumeric.sample_string(&mut rng, size);
        local.insert(item.clone());
        remote.insert(item);
    }

    for _ in 0..diff_items {
        let item = Alphanumeric.sample_string(&mut rng, size);
        local.insert(item);

        let item = Alphanumeric.sample_string(&mut rng, size);
        remote.insert(item);
    }

    let expected_diffs = local
        .elements()
        .symmetric_difference(remote.elements())
        .count();
    assert_eq!(expected_diffs, 2 * diff_items);

    (local, remote)
}

/// Runs the specified protocol and outputs the metrics obtained.
fn run<P>(
    protocol: &mut P,
    id: Option<&str>,
    similarity: usize,
    download: NetworkBandwitdth,
    upload: NetworkBandwitdth,
) where
    P: Protocol<Tracker = DefaultTracker>,
{
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
        "{type_name} {similarity} {hops} {:.3} {bytes}",
        duration.as_secs_f64()
    );
}

fn main() {
    let execution_time = Instant::now();

    let (item_count, item_size, seed) = (100_000, 80, random());
    let (download, upload) = (NetworkBandwitdth::Mbps(10.0), NetworkBandwitdth::Mbps(10.0));
    println!(
        "{item_count} {item_size} {seed} {} {}",
        download.as_bytes_per_sec() as usize,
        upload.as_bytes_per_sec() as usize,
    );

    let (start, end, step) = (0, 100, 10);
    println!("{start} {end} {step}");

    for similarity in (start..=end).rev().step_by(10) {
        let (local, remote) = populate(item_count, item_size, similarity, seed);

        let mut baseline = Baseline::builder(local.clone(), remote.clone()).build();
        run(&mut baseline, None, similarity, download, upload);
    }

    eprintln!("time elapsed: {:.3?}", execution_time.elapsed());
}
