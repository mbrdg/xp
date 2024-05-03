#![allow(dead_code)]

use crate::{
    crdt::GSet,
    sync::{Baseline, BucketDispatcher, Probabilistic, Protocol},
    tracker::{NetworkHop, Tracker},
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

fn populate_replicas(
    count: usize,
    size: usize,
    similarity: usize,
    seed: u64,
) -> (GSet<String>, GSet<String>) {
    let similar_items = (count as f64 * (similarity as f64 / 100.0)) as usize;
    let diff_items = count - similar_items;

    let mut gsets = (GSet::new(), GSet::new());
    let mut rng = StdRng::seed_from_u64(seed);

    for _ in 0..similar_items {
        let item = Alphanumeric.sample_string(&mut rng, size);
        gsets.0.insert(item.clone());
        gsets.1.insert(item);
    }

    for _ in 0..diff_items {
        let item = Alphanumeric.sample_string(&mut rng, size);
        gsets.0.insert(item);

        let item = Alphanumeric.sample_string(&mut rng, size);
        gsets.1.insert(item);
    }

    assert_eq!(
        gsets
            .0
            .elements()
            .symmetric_difference(&gsets.1.elements())
            .count(),
        2 * diff_items
    );

    gsets
}

fn main() {
    let (item_count, item_size, seed) = (100_000, 80, random());
    println!("{:?}", (item_count, item_size, seed));

    println!("Algorithm\tSimilar\tDiffs\tHops\tBytes (ratio)");

    for similarity in (0..=100).rev().step_by(1) {
        let (local, remote) = populate_replicas(item_count, item_size, similarity, seed);

        let baseline = Baseline::new(local.clone(), remote.clone()).sync();
        let (diffs, bytes, hops) = (
            baseline.differences().unwrap(),
            baseline
                .events()
                .iter()
                .map(NetworkHop::bytes)
                .sum::<usize>(),
            baseline.events().len(),
        );
        println!(
            "Baseline\t{similarity}\t{diffs}\t{hops}\t{bytes} ({:.5}%)",
            bytes as f32 / (item_count * item_size) as f32 * 100.0
        );

        let probabilistic = Probabilistic::new(local.clone(), remote.clone()).sync();
        let (diffs, bytes, hops) = (
            probabilistic.differences().unwrap(),
            probabilistic
                .events()
                .iter()
                .map(NetworkHop::bytes)
                .sum::<usize>(),
            probabilistic.events().len(),
        );
        println!(
            "Probabilistic\t{similarity}\t{diffs}\t{hops}\t{bytes} ({:.5}%)",
            bytes as f32 / (item_count * item_size) as f32 * 100.0
        );

        let dispatcher_16 = BucketDispatcher::<16>::new(local.clone(), remote.clone()).sync();
        let (diffs, bytes, hops) = (
            dispatcher_16.differences().unwrap(),
            dispatcher_16
                .events()
                .iter()
                .map(NetworkHop::bytes)
                .sum::<usize>(),
            dispatcher_16.events().len(),
        );
        println!(
            "Buckets<16>\t{similarity}\t{diffs}\t{hops}\t{bytes} ({:.5}%)",
            bytes as f32 / (item_count * item_size) as f32 * 100.0
        );

        let dispatcher_64 = BucketDispatcher::<64>::new(local, remote).sync();
        let (diffs, bytes, hops) = (
            dispatcher_64.differences().unwrap(),
            dispatcher_64
                .events()
                .iter()
                .map(NetworkHop::bytes)
                .sum::<usize>(),
            dispatcher_64.events().len(),
        );
        println!(
            "Buckets<64>\t{similarity}\t{diffs}\t{hops}\t{bytes} ({:.5}%)",
            bytes as f32 / (item_count * item_size) as f32 * 100.0
        );
    }
}
