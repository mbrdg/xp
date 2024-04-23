use crate::{crdt::GSet, sync::Baseline};
use rand::{
    distributions::{Alphanumeric, DistString},
    rngs::StdRng,
    SeedableRng,
};
use sync::Algorithm;

mod crdt;
mod sync;

#[derive(Debug)]
pub struct Config {
    item_count: usize,
    item_size: usize,
    similarity: u8,
    seed: u64,
}

fn gen_items(config: Config) -> (GSet<String>, GSet<String>) {
    let sim_items = config.item_count * usize::from(config.similarity) / 100;
    let diff_items = config.item_count - sim_items;

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
        item_count: 10000,
        item_size: 80,
        similarity: 50,
        seed: 42,
    };

    assert!(
        (0..=100).contains(&config.similarity),
        "similariry must be a percentage, i.e, a value between 0 and 100",
    );
    println!("{:?}", config);

    let (local, remote) = gen_items(config);
    let mut baseline = Baseline::new(local, remote);
    println!("{:?}", baseline.sync());
}
