use rand::distributions::{Alphanumeric, DistString};
use xp::GSet;

#[derive(Debug)]
struct Config {
    item_count: usize,
    item_size: usize,
    similarity: u8,
}

fn fill(config: &Config) -> (GSet<String>, GSet<String>) {
    assert!(
        (0..101).contains(&config.similarity),
        "similariry given is not a percentage, got {}",
        config.similarity
    );

    let sim_items = config.item_count * usize::from(config.similarity) / 100;
    let diff_items = config.item_count - sim_items;

    let mut gsets = (GSet::new(), GSet::new());

    let mut rng = rand::thread_rng();

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

fn sync<T>(primary: &mut GSet<T>, secondary: &mut GSet<T>) {
    todo!()
}

fn main() {
    let config = Config {
        item_count: 10,
        item_size: 5,
        similarity: 100,
    };

    let (mut local, mut remote) = fill(&config);
    println!("{:?}", config);
    println!("{:?}", local);
    println!("{:?}", remote);

    sync(&mut local, &mut remote);
}
