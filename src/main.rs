#![allow(dead_code)]

use std::f64;

use crate::{
    crdt::GSet,
    sync::{Baseline, PSync},
};
use rand::{
    distributions::{Alphanumeric, DistString},
    rngs::StdRng,
    SeedableRng,
};
use sync::Algorithm;

mod bloom;
mod crdt;
mod sync;

#[derive(Debug)]
pub struct Config {
    item_count: usize,
    item_size: usize,
    similarity: f64,
    seed: u64,
}

fn gen_items(config: Config) -> (GSet<String>, GSet<String>) {
    println!("Generating items: {:?}", config);
    let sim_items = (config.item_count as f64 * config.similarity) as usize;
    let diff_items = config.item_count - sim_items;

    dbg!(sim_items, diff_items);

    let mut gsets = (GSet::new(), GSet::new());

    let mut rng = StdRng::seed_from_u64(config.seed);

    for _ in 0..sim_items {
        let item = Alphanumeric.sample_string(&mut rng, config.item_size);
        gsets.0.insert(item.clone());
        gsets.1.insert(item);
    }

    for _ in 0..diff_items {
        let item = Alphanumeric.sample_string(&mut rng, config.item_size);
        gsets.0.insert(item);

        let item = Alphanumeric.sample_string(&mut rng, config.item_size);
        gsets.1.insert(item);
    }

    gsets
}

fn main() {
    let config = Config {
        item_count: 10_000,
        item_size: 80,
        similarity: 0.90,
        seed: 42,
    };

    assert!(
        (0.0..=1.0).contains(&config.similarity),
        "similarity should be in the interval between 0.0 and 1.0 inclusive"
    );

    let (local, remote) = gen_items(config);

    let mut baseline = Baseline::new(local.clone(), remote.clone());
    baseline.sync();

    let mut probabilistic = PSync::new(local, remote);
    probabilistic.sync();

    // let mut dispatcher = BucketDispatcher::<64>::new(local, remote);
    // dispatcher.sync();
}
