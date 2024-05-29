#![allow(dead_code)]

use std::{
    any,
    time::{Duration, Instant},
};

use crate::{
    crdt::GSet,
    sync::{Baseline, BloomBuckets, Buckets, Protocol},
    tracker::{NetworkBandwitdth, NetworkEvent},
};
use rand::{
    distributions::{Alphanumeric, DistString},
    random,
    rngs::StdRng,
    SeedableRng,
};
use tracker::{DefaultTracker, Tracker};

mod bloom;
mod crdt;
mod sync;
mod tracker;

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

    assert_eq!(
        local
            .elements()
            .symmetric_difference(remote.elements())
            .count(),
        2 * diff_items
    );

    (local, remote)
}

/// Runs the specified protocol and outputs the metrics obtained.
/// NOTE: The values for download and upload are expressed in Bytes/s.
fn run<P>(protocol: &mut P, id: Option<&str>, similarity: usize, download: usize, upload: usize)
where
    P: Protocol<Tracker = DefaultTracker<NetworkEvent>>,
{
    let mut tracker = DefaultTracker::new(download, upload);
    protocol.sync(&mut tracker);

    let type_name = {
        let name = any::type_name_of_val(&protocol).split("::").last().unwrap();
        match id {
            Some(id) => format!("{name}<{id}>"),
            None => name.to_string(),
        }
    };

    if !tracker.is_synced() {
        eprintln!(
            "{type_name} not totally synced with {} diffs",
            tracker.diffs().unwrap()
        );
    }

    let hops = tracker.events().len();
    let duration: Duration = tracker.events().iter().map(NetworkEvent::duration).sum();
    let bytes: usize = tracker.events().iter().map(NetworkEvent::bytes).sum();

    println!(
        "{type_name} {similarity} {hops} {:.3} {bytes}",
        duration.as_secs_f64()
    );
}

fn main() {
    let start = Instant::now();

    let (item_count, item_size, seed) = (100_000, 80, random());
    let (download, upload) = (NetworkBandwitdth::MiB(64), NetworkBandwitdth::MiB(64));
    println!(
        "{item_count} {item_size} {seed} {} {}",
        download.bytes_per_sec(),
        upload.bytes_per_sec()
    );

    let (similarity, step) = (0..=100, 5);
    println!("{} {} {step}", similarity.start(), similarity.end());

    for similarity_factor in similarity.rev().step_by(5) {
        let (local, remote) = populate(item_count, item_size, similarity_factor, seed);

        run(
            &mut Baseline::new(local.clone(), remote.clone()),
            None,
            similarity_factor,
            download.bytes_per_sec(),
            upload.bytes_per_sec(),
        );

        let load_factors = [1.0, 1.25];
        for load_factor in load_factors {
            run(
                &mut Buckets::with_load_factor(local.clone(), remote.clone(), load_factor),
                Some(&load_factor.to_string()),
                similarity_factor,
                download.bytes_per_sec(),
                upload.bytes_per_sec(),
            );
        }

        run(
            &mut BloomBuckets::new(local.clone(), remote.clone()),
            None,
            similarity_factor,
            download.bytes_per_sec(),
            upload.bytes_per_sec(),
        );
    }

    eprintln!("time elapsed {:.3?}", start.elapsed());
}
