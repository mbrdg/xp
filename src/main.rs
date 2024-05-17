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

    println!(
        "{protocol} {similarity} {hops} {:.3} {bytes}",
        duration.as_secs_f64()
    )
}

fn main() {
    let start = Instant::now();

    let (item_count, item_size, seed) = (100_000, 80, random());
    println!("{item_count} {item_size} {seed}");

    let (similarity, step) = (0..=100, 5);
    println!("{} {} {step}", similarity.start(), similarity.end());

    for s in similarity.rev().step_by(5) {
        let (local, remote) = populate_replicas(item_count, item_size, s, seed);

        let baseline = Baseline::new(local.clone(), remote.clone()).sync();
        print_stats("baseline", s, baseline);

        // NOTE: The number of buckets must increase accordingly to the set's size.
        let buckets_1024 = Buckets::<1024>::new(local.clone(), remote.clone()).sync();
        print_stats("buckets<1024>", s, buckets_1024);

        let buckets_2048 = Buckets::<2048>::new(local.clone(), remote.clone()).sync();
        print_stats("buckets<2048>", s, buckets_2048);

        let buckets_4096 = Buckets::<4096>::new(local.clone(), remote.clone()).sync();
        print_stats("buckets<4096>", s, buckets_4096);

        let buckets_8192 = Buckets::<8192>::new(local.clone(), remote.clone()).sync();
        print_stats("buckets<8192>", s, buckets_8192);
    }

    eprintln!("time elapsed {:.3?}", start.elapsed());
}
