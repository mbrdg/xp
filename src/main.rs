#![allow(dead_code)]

use std::time::{Duration, Instant};

use crate::{
    crdt::{AWSet, GSet, Measurable},
    sync::{baseline::Baseline, bloombuckets::BloomBuckets, buckets::Buckets, Protocol},
    tracker::{Bandwidth, DefaultEvent, DefaultTracker, Tracker},
};

use crdt::{Decomposable, Extractable};
use rand::{
    distributions::{Alphanumeric, DistString, Distribution, Uniform},
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

    let mut local = GSet::new();
    let mut remote = GSet::new();

    Uniform::from(50..=80)
        .sample_iter(rng.clone())
        .map(|len| Alphanumeric.sample_string(rng, len))
        .take(sims)
        .for_each(|item| {
            local.insert(item.clone());
            remote.insert(item);
        });

    Uniform::from(50..=80)
        .sample_iter(rng.clone())
        .map(|len| Alphanumeric.sample_string(rng, len))
        .take(diffs)
        .for_each(|item| {
            local.insert(item);
        });

    Uniform::from(50..=80)
        .sample_iter(rng.clone())
        .map(|len| Alphanumeric.sample_string(rng, len))
        .take(diffs)
        .for_each(|item| {
            remote.insert(item);
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
    let diffs = len - sims;

    let mut common = AWSet::new();
    Uniform::from(50..=80)
        .sample_iter(rng.clone())
        .map(|len| Alphanumeric.sample_string(rng, len))
        .take(sims)
        .for_each(|item| {
            common.insert(item);
        });

    let sims_dels = (sims as f64 * del) as usize;
    let diff_dels = (diffs as f64 * del) as usize;

    common
        .elements()
        .cloned()
        .choose_multiple(rng, sims_dels)
        .into_iter()
        .for_each(|item| {
            common.remove(&item);
        });

    let mut local_only = AWSet::new();
    Uniform::from(50..=80)
        .sample_iter(rng.clone())
        .map(|len| Alphanumeric.sample_string(rng, len))
        .take(diffs)
        .for_each(|item| {
            local_only.insert(item);
        });

    local_only
        .elements()
        .cloned()
        .choose_multiple(rng, diff_dels)
        .into_iter()
        .for_each(|item| {
            local_only.remove(&item);
        });

    let mut remote_only = AWSet::new();
    Uniform::from(50..=80)
        .sample_iter(rng.clone())
        .map(|len| Alphanumeric.sample_string(rng, len))
        .take(diffs)
        .for_each(|item| {
            remote_only.insert(item);
        });

    remote_only
        .elements()
        .cloned()
        .choose_multiple(rng, diff_dels)
        .into_iter()
        .for_each(|item| {
            remote_only.remove(&item);
        });

    let mut local = common.clone();
    local.join(vec![local_only]);

    let mut remote = common.clone();
    remote.join(vec![remote_only]);

    assert_eq!(local.len(), remote.len());
    (local, remote)
}

/// Runs the specified protocol and outputs the metrics obtained.
fn run<P>(proto: &mut P, id: &str, similar: f64, download: Bandwidth, upload: Bandwidth)
where
    P: Protocol<Tracker = DefaultTracker>,
{
    assert!(
        (0.0..=1.0).contains(&similar),
        "similarity should be a ratio between 0.0 and 1.0"
    );

    let mut tracker = DefaultTracker::new(download, upload);
    proto.sync(&mut tracker);

    let diffs = tracker.diffs();
    if diffs > 0 {
        eprintln!("{id} not totally synced with {diffs} diffs");
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
    T: Clone + Decomposable<Decomposition = T> + Default + Extractable + Measurable,
{
    let size_of_local = <T as Measurable>::size_of(&local);
    let size_of_remote = <T as Measurable>::size_of(&remote);
    let links = [
        (Bandwidth::Mbps(10.0), Bandwidth::Mbps(1.0)),
        (Bandwidth::Mbps(10.0), Bandwidth::Mbps(10.0)),
        (Bandwidth::Mbps(1.0), Bandwidth::Mbps(10.0)),
    ];

    for (upload, download) in links {
        println!(
            "\n{size_of_local} {} {size_of_remote} {}",
            upload.bits_per_sec(),
            download.bits_per_sec()
        );

        let mut protocol = Baseline::new(local.clone(), remote.clone());
        run(&mut protocol, "Baseline", similar, download, upload);

        for load in [0.2, 1.0, 5.0] {
            let id = format!("Buckets[lf={load}]");
            let num_buckets = (load * <T as Measurable>::len(&local) as f64) as usize;
            let mut protocol = Buckets::new(local.clone(), remote.clone(), num_buckets);

            run(&mut protocol, &id, similar, download, upload);
        }

        for fpr in [0.2, 1.0, 5.0] {
            let id = format!("BloomBuckets[lf=1.0,fpr={fpr:.1}%]");
            let num_buckets = <T as Measurable>::len(&local);
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
    eprintln!("[{:?}] got seed with value of {seed}", exec_time.elapsed());

    (0..=100).step_by(10).for_each(|similar| {
        let s = f64::from(similar) / 100.0;
        let (local, remote) = gsets_with(100_000, s, &mut rng);
        run_with(s, local, remote)
    });
    eprintln!("[{:?}] gsets done, going for awsets", exec_time.elapsed());

    // NOTE: AWSets generated with 20% of elements removed. This value is pretty conservative for
    // the particular study scenario of 15% of deleted or removed posts as in mainstream social
    // media [1].
    //
    // [1]: https://www.researchgate.net/publication/367503309_Engagement_with_fact-checked_posts_on_Reddit
    (0..=100).step_by(10).for_each(|similar| {
        let s = f64::from(similar) / 100.0;
        let (local, remote) = awsets_with(25_000, s, 0.2, &mut rng);
        run_with(s, local, remote)
    });
    eprintln!("[{:.?}] awsets done, exiting...", exec_time.elapsed());
}
