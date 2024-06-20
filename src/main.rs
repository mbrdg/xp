#![allow(dead_code)]

use std::{
    env,
    time::{Duration, Instant},
};

use crate::{
    crdt::{AWSet, GSet, Measure},
    sync::{baseline::Baseline, bloombuckets::BloomBuckets, buckets::Buckets, Algorithm},
    tracker::{Bandwidth, DefaultEvent, DefaultTracker, Telemetry},
};

use crdt::{Decompose, Extract};
use rand::{
    distributions::{Alphanumeric, DistString},
    rngs::StdRng,
    seq::IteratorRandom,
    SeedableRng,
};

mod bloom;
mod crdt;
mod sync;
mod tracker;

fn gsets_with(len: usize, similar: f64, rng: &mut StdRng) -> (GSet<String>, GSet<String>) {
    assert!(
        (0.0..=1.0).contains(&similar),
        "similarity ratio should be in (0.0..=1.0)"
    );

    let sims = (len as f64 * similar) as usize;
    let diffs = len - sims;

    let mut common = GSet::new();
    (0..sims).for_each(|_| {
        common.insert(Alphanumeric.sample_string(rng, 80));
    });

    let mut local = common.clone();
    (0..diffs).for_each(|_| {
        local.insert(Alphanumeric.sample_string(rng, 80));
    });

    let mut remote = common.clone();
    (0..diffs).for_each(|_| {
        remote.insert(Alphanumeric.sample_string(rng, 80));
    });

    assert_eq!(local.len(), len);
    assert_eq!(remote.len(), len);
    assert_eq!(local.false_matches(&remote), 2 * diffs);
    (local, remote)
}

fn awsets_with(
    len: usize,
    similar: f64,
    del: f64,
    rng: &mut StdRng,
) -> (AWSet<String>, AWSet<String>) {
    assert!(
        (0.0..=1.0).contains(&similar),
        "similarity ratio should be in (0.0..=1.0)"
    );
    assert!(
        (0.0..0.95).contains(&del),
        "deletion ratio should be in the in (0.0..0.95)"
    );

    let sims = (len as f64 * similar) as usize;
    let sims_dels = (sims as f64 * del) as usize;

    let mut common = AWSet::new();
    (0..sims).for_each(|_| {
        common.insert(Alphanumeric.sample_string(rng, 80));
    });

    common
        .elements()
        .cloned()
        .choose_multiple(rng, sims_dels)
        .iter()
        .for_each(|item| {
            common.remove(item);
        });

    let diffs = len - sims;
    let diff_dels = (diffs as f64 * del) as usize;

    let mut local = AWSet::new();
    (0..diffs).for_each(|_| {
        local.insert(Alphanumeric.sample_string(rng, 80));
    });

    local
        .elements()
        .cloned()
        .choose_multiple(rng, diff_dels)
        .iter()
        .for_each(|item| {
            local.remove(item);
        });

    local.join(vec![common.clone()]);

    let mut remote = AWSet::new();
    (0..diffs).for_each(|_| {
        remote.insert(Alphanumeric.sample_string(rng, 80));
    });

    remote
        .elements()
        .cloned()
        .choose_multiple(rng, diff_dels)
        .iter()
        .for_each(|item| {
            remote.remove(item);
        });

    remote.join(vec![common]);

    assert_eq!(local.len(), (len as f64 * (1.0 - del)) as usize);
    assert_eq!(remote.len(), (len as f64 * (1.0 - del)) as usize);
    assert_eq!(local.false_matches(&remote), 2 * (diffs - diff_dels));
    (local, remote)
}

/// Runs the specified protocol and outputs the metrics obtained.
fn run<A>(algo: &mut A, id: &str, similar: f64, download: Bandwidth, upload: Bandwidth)
where
    A: Algorithm<Tracker = DefaultTracker>,
{
    assert!(
        (0.0..=1.0).contains(&similar),
        "similarity should be a ratio between 0.0 and 1.0"
    );

    let mut tracker = DefaultTracker::new(download, upload);
    algo.sync(&mut tracker);

    let diffs = tracker.false_matches();
    if diffs > 0 {
        eprintln!("{id} not totally synced with {diffs} false matches");
    }

    let events = tracker.events();
    println!(
        "{id} {} {} {:.3}",
        events.iter().map(DefaultEvent::state).sum::<usize>(),
        events.iter().map(DefaultEvent::metadata).sum::<usize>(),
        events
            .iter()
            .filter_map(|e| e.duration().ok())
            .sum::<Duration>()
            .as_secs_f64(),
    );
}

fn run_with<T>(similar: f64, local: T, remote: T)
where
    T: Clone + Decompose<Decomposition = T> + Default + Extract + Measure,
{
    let size = <T as Measure>::size_of(&local);
    assert_eq!(size, <T as Measure>::size_of(&remote));

    let links = [
        (Bandwidth::Mbps(10.0), Bandwidth::Mbps(1.0)),
        (Bandwidth::Mbps(10.0), Bandwidth::Mbps(10.0)),
        (Bandwidth::Mbps(1.0), Bandwidth::Mbps(10.0)),
    ];

    for (upload, download) in links {
        println!(
            "\n{size} {} {}",
            upload.bits_per_sec(),
            download.bits_per_sec()
        );

        let mut protocol = Baseline::new(local.clone(), remote.clone());
        run(&mut protocol, "Baseline", similar, download, upload);

        for load in [0.2, 1.0, 5.0] {
            let id = format!("Bucketing[lf={load}]");
            let num_buckets = (load * <T as Measure>::len(&local) as f64) as usize;
            let mut protocol = Buckets::new(local.clone(), remote.clone(), num_buckets);

            run(&mut protocol, &id, similar, download, upload);
        }

        for fpr in [1.0, 25.0] {
            let id = format!("Bloom+Bucketing[lf=1,fpr={fpr}%]");
            let num_buckets = <T as Measure>::len(&local);
            let mut protocol =
                BloomBuckets::new(local.clone(), remote.clone(), fpr / 100.0, num_buckets);

            run(&mut protocol, &id, similar, download, upload);
        }
    }
}

/// Entry point for the execution of the experiments.
///
/// The first experiment is on similar uses replica with the same cardinality and a given degree of
/// similarity. Furthermore, we repeat this experiment on different link configurations with both
/// symmetric and assymetric channels.
///
/// The second experiment is on replicas of distinct cardinalities, also with different link
/// configurations, again, with both symmetric and asymmetric channels.
fn main() {
    let exec_time = Instant::now();

    let seed = rand::random();
    let mut rng = StdRng::seed_from_u64(seed);
    eprintln!(
        "[{:.2?}] got seed with value of {seed}",
        exec_time.elapsed()
    );

    let args = env::args().collect::<Vec<_>>();
    assert_eq!(args.len(), 2);

    let similarities = (0..=100)
        .step_by(5)
        .map(|similar| f64::from(similar) / 100.0);

    match args[1].to_lowercase().as_str() {
        "gset" => similarities.for_each(|s| {
            let (local, remote) = gsets_with(100_000, s, &mut rng);
            eprintln!(
                "[{:.2?}] gsets with similarity of {s} generated",
                exec_time.elapsed()
            );

            run_with(s, local, remote);
        }),

        // NOTE: AWSets generated with 20% of elements removed. This value is pretty conservative for
        // the particular study scenario of 15% of deleted or removed posts as in mainstream social
        // media [1].
        //
        // [1]: https://www.researchgate.net/publication/367503309_Engagement_with_fact-checked_posts_on_Reddit
        "awset" => similarities.for_each(|s| {
            let (local, remote) = awsets_with(20_000, s, 0.2, &mut rng);
            eprintln!(
                "[{:.2?}] awsets with similarity of {s} generated",
                exec_time.elapsed()
            );

            run_with(s, local, remote);
        }),
        _ => unreachable!(),
    };

    eprintln!("[{:.2?}] exiting...", exec_time.elapsed());
}
