#![allow(dead_code)]

use std::time::{Duration, Instant};

use crate::{
    crdt::GSet,
    sync::{Baseline, Buckets, Protocol},
    tracker::{EventTracker, NetworkHop},
};
use rand::{
    distributions::{Alphanumeric, DistString},
    random,
    rngs::StdRng,
    SeedableRng,
};
use tracker::DefaultTracker;

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
    eprintln!("similarity: {similarity} ({similar_items}, {diff_items})");

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
    let (hops, duration, bytes) = (
        tracker.events().len(),
        tracker
            .events()
            .iter()
            .map(NetworkHop::duration)
            .sum::<Duration>(),
        tracker
            .events()
            .iter()
            .map(NetworkHop::bytes)
            .sum::<usize>(),
    );

    println!("{protocol} {similarity} {hops} {duration:?} {bytes}")
}

fn main() {
    let start = Instant::now();

    let (item_count, item_size, seed) = (100_000, 80, random());
    eprintln!("config: {item_count} {item_size} {seed}");

    for similarity in (0..=100).rev().step_by(5) {
        let (local, remote) = populate_replicas(item_count, item_size, similarity, seed);

        let baseline = Baseline::new(local.clone(), remote.clone()).sync();
        print_stats("baseline", similarity, baseline);

        // NOTE: The number of buckets must increase accordingly to the set's size.
        let buckets_1024 = Buckets::<1024>::new(local.clone(), remote.clone()).sync();
        print_stats("buckets_1024", similarity, buckets_1024);

        let buckets_2048 = Buckets::<2048>::new(local.clone(), remote.clone()).sync();
        print_stats("buckets_2048", similarity, buckets_2048);

        let buckets_4096 = Buckets::<4096>::new(local.clone(), remote.clone()).sync();
        print_stats("buckets_4096", similarity, buckets_4096);

        let buckets_8192 = Buckets::<8192>::new(local.clone(), remote.clone()).sync();
        print_stats("buckets_8192", similarity, buckets_8192);
    }

    eprintln!("Took {:.3?}", start.elapsed());
}
