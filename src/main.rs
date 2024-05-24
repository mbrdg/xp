#![allow(dead_code)]

use std::time::{Duration, Instant};

use crate::{
    crdt::GSet,
    sync::{Baseline, Buckets, Protocol},
    tracker::NetworkEvent,
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

fn populate_replicas(
    count: usize,
    size: usize,
    similarity: usize,
    seed: u64,
) -> (GSet<String>, GSet<String>) {
    let similar_items = (count as f64 * (similarity as f64 / 100.0)) as usize;
    let diff_items = count - similar_items;

    let (mut l, mut r) = (GSet::new(), GSet::new());
    let mut rng = StdRng::seed_from_u64(seed);

    for _ in 0..similar_items {
        let item = Alphanumeric.sample_string(&mut rng, size);
        l.insert(item.clone());
        r.insert(item);
    }

    for _ in 0..diff_items {
        let item = Alphanumeric.sample_string(&mut rng, size);
        l.insert(item);

        let item = Alphanumeric.sample_string(&mut rng, size);
        r.insert(item);
    }

    assert_eq!(
        l.elements().symmetric_difference(r.elements()).count(),
        2 * diff_items
    );

    (l, r)
}

fn print_stats(protocol: &str, similarity: usize, tracker: DefaultTracker) {
    let hops = tracker.events().len();
    let duration: Duration = tracker.events().iter().map(NetworkEvent::duration).sum();
    let bytes: usize = tracker.events().iter().map(NetworkEvent::bytes).sum();

    println!(
        "{protocol} {similarity} {hops} {:.3} {bytes}",
        duration.as_secs_f64()
    )
}

fn main() {
    let start = Instant::now();

    let (item_count, item_size, seed) = (100_000, 80, random());
    let (download, upload) = (32_000, 32_000); // NOTE: These values are in Bytes/s.
    println!("{item_count} {item_size} {seed} {download} {upload}");

    let (similarity, step) = (0..=100, 5);
    println!("{} {} {step}", similarity.start(), similarity.end());

    for s in similarity.rev().step_by(5) {
        let (local, remote) = populate_replicas(item_count, item_size, s, seed);

        let baseline = {
            let mut tracker = DefaultTracker::new(download, upload);
            Baseline::new(local.clone(), remote.clone()).sync(&mut tracker);
            tracker
        };

        print_stats("baseline", s, baseline);

        // NOTE: The number of buckets should increase accordingly to the set's size.
        let buckets_5k = {
            let mut tracker = DefaultTracker::new(download, upload);
            Buckets::<5_000>::new(local.clone(), remote.clone()).sync(&mut tracker);
            tracker
        };

        print_stats("buckets<5k>", s, buckets_5k);

        let buckets_10k = {
            let mut tracker = DefaultTracker::new(download, upload);
            Buckets::<10_000>::new(local.clone(), remote.clone()).sync(&mut tracker);
            tracker
        };

        print_stats("buckets<10k>", s, buckets_10k);

        let buckets_20k = {
            let mut tracker = DefaultTracker::new(download, upload);
            Buckets::<20_000>::new(local.clone(), remote.clone()).sync(&mut tracker);
            tracker
        };

        print_stats("buckets<20k>", s, buckets_20k);

        let buckets_50k = {
            let mut tracker = DefaultTracker::new(download, upload);
            Buckets::<50_000>::new(local.clone(), remote.clone()).sync(&mut tracker);
            tracker
        };

        print_stats("buckets<50k>", s, buckets_50k);
    }

    eprintln!("time elapsed {:.3?}", start.elapsed());
}
