#![allow(dead_code)]

use std::{
    ops::Range,
    time::{Duration, Instant},
};

use crate::{
    crdt::{GSet, Measurable},
    sync::{
        baseline::Baseline, bloombuckets::BloomBuckets, buckets::Buckets, Bloomer,
        BucketDispatcher, Protocol,
    },
    tracker::{DefaultTracker, NetworkBandwitdth, NetworkEvent, Tracker},
};

use rand::{
    distributions::{Alphanumeric, DistString, Distribution, Uniform},
    rngs::StdRng,
    SeedableRng,
};

mod bloom;
mod crdt;
mod sync;
mod tracker;

fn spawn_similar_gsets(
    cardinality: usize,
    lengths: Range<usize>,
    similarity: f64,
    rng: &mut StdRng,
) -> (GSet<String>, GSet<String>) {
    assert!(
        (0.0..=1.0).contains(&similarity),
        "similarity should be a ratio between 0.0 and 1.0"
    );

    let num_similar_items = (cardinality as f64 * similarity) as usize;
    let num_diff_items = cardinality - num_similar_items;

    let mut gsets = (GSet::new(), GSet::new());

    // Generate common items for both gsets
    Uniform::from(lengths.clone())
        .sample_iter(rng.clone())
        .map(|len| Alphanumeric.sample_string(rng, len))
        .take(num_similar_items)
        .for_each(|item| {
            gsets.0.insert(item.clone());
            gsets.1.insert(item);
        });

    // Generate diff items for the first gset
    Uniform::from(lengths.clone())
        .sample_iter(rng.clone())
        .map(|len| Alphanumeric.sample_string(rng, len))
        .take(num_diff_items)
        .for_each(|item| {
            gsets.0.insert(item);
        });

    // Generate diff items for the second gset
    Uniform::from(lengths)
        .sample_iter(rng.clone())
        .map(|len| Alphanumeric.sample_string(rng, len))
        .take(num_diff_items)
        .for_each(|item| {
            gsets.1.insert(item);
        });

    // Ensure that each set has exactly the appropriate number of distinct items
    assert_eq!(cardinality, gsets.0.len());
    assert_eq!(cardinality, gsets.1.len());
    assert_eq!(gsets.0.false_matches(&gsets.1), 2 * num_diff_items);

    gsets
}

#[derive(Debug)]
struct ReplicaStatus(usize, NetworkBandwitdth);

/// Runs the specified protocol and outputs the metrics obtained.
fn run<P>(
    protocol: &mut P,
    type_name: &str,
    similarity: f64,
    ReplicaStatus(size_of_remote, download): &ReplicaStatus,
    ReplicaStatus(size_of_local, upload): &ReplicaStatus,
) where
    P: Protocol<Tracker = DefaultTracker>,
{
    assert!(
        (0.0..=1.0).contains(&similarity),
        "similarity should be a ratio between 0.0 and 1.0"
    );

    let mut tracker = DefaultTracker::new(*download, *upload);
    protocol.sync(&mut tracker);

    let diffs = tracker.diffs();
    if diffs > 0 {
        eprintln!("{type_name} not totally synced with {diffs} diffs");
    }

    let events = tracker.events();
    let duration = events.iter().map(NetworkEvent::duration).sum::<Duration>();
    let transferred = events.iter().map(NetworkEvent::bytes).sum::<usize>();

    println!(
        "{type_name} {size_of_local} {size_of_remote} {} {} {transferred} {:.3}",
        download.as_bytes_per_sec(),
        upload.as_bytes_per_sec(),
        duration.as_secs_f64()
    );
}

type SimilarReplicas<T> = Vec<(f64, (T, ReplicaStatus), (T, ReplicaStatus))>;
fn exec_similar(replicas: &SimilarReplicas<GSet<String>>) {
    // Baseline
    replicas.iter().for_each(|(s, local, remote)| {
        let mut protocol = Baseline::new(local.0.clone(), remote.0.clone());
        run(&mut protocol, "Baseline", *s, &local.1, &remote.1);
    });

    // Buckets<0.2>
    replicas.iter().for_each(|(s, local, remote)| {
        let dispatcher = BucketDispatcher::new((0.2 * local.0.len() as f64) as usize);
        let mut protocol = Buckets::new(local.0.clone(), remote.0.clone(), dispatcher);
        run(&mut protocol, "Buckets [lf=0.2]", *s, &local.1, &remote.1);
    });

    // Buckets<1.0>
    replicas.iter().for_each(|(s, local, remote)| {
        let dispatcher = BucketDispatcher::new(local.0.len());
        let mut protocol = Buckets::new(local.0.clone(), remote.0.clone(), dispatcher);
        run(&mut protocol, "Buckets [lf=1.0]", *s, &local.1, &remote.1);
    });

    // Buckets<5.0>
    replicas.iter().for_each(|(s, local, remote)| {
        let dispatcher = BucketDispatcher::new(5 * local.0.len());
        let mut protocol = Buckets::new(local.0.clone(), remote.0.clone(), dispatcher);
        run(&mut protocol, "Buckets [lf=5.0]", *s, &local.1, &remote.1);
    });

    // BloomBuckets<1.0, 0.01>
    replicas.iter().for_each(|(s, local, remote)| {
        let bloomer = Bloomer::new(0.01);
        let dispatcher = BucketDispatcher::new(local.0.len());
        let mut protocol =
            BloomBuckets::new(local.0.clone(), remote.0.clone(), bloomer, dispatcher);
        run(
            &mut protocol,
            "BloomBuckets [fpr=1%,lf=1.0]",
            *s,
            &local.1,
            &remote.1,
        );
    });

    // BloomBuckets<1.0, 0.025>
    replicas.iter().for_each(|(s, local, remote)| {
        let bloomer = Bloomer::new(0.05);
        let dispatcher = BucketDispatcher::new(local.0.len());
        let mut protocol =
            BloomBuckets::new(local.0.clone(), remote.0.clone(), bloomer, dispatcher);
        run(
            &mut protocol,
            "BloomBuckets [fpr=5%,lf=1.0]",
            *s,
            &local.1,
            &remote.1,
        );
    });
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
    let execution_time = Instant::now();

    let seed = rand::random();
    let mut rng = StdRng::seed_from_u64(seed);
    println!("seed={seed}");

    let mut replicas = (0..=100)
        .step_by(10)
        .map(|s| {
            let similarity = f64::from(s) / 100.0;
            let (local, remote) = spawn_similar_gsets(100_000, 50..80, similarity, &mut rng);

            let local_status = ReplicaStatus(GSet::size_of(&local), NetworkBandwitdth::Mbps(10.0));
            let remote_status =
                ReplicaStatus(GSet::size_of(&remote), NetworkBandwitdth::Mbps(10.0));

            (similarity, (local, local_status), (remote, remote_status))
        })
        .collect::<Vec<_>>();

    // Upload == Download
    println!();
    exec_similar(&replicas);

    // Upload << Download
    replicas
        .iter_mut()
        .for_each(|(_, (_, local_status), (_, remote_status))| {
            local_status.1 = NetworkBandwitdth::Mbps(1.0);
            remote_status.1 = NetworkBandwitdth::Mbps(10.0);
        });

    println!();
    exec_similar(&replicas);

    // Upload >> Download
    replicas
        .iter_mut()
        .for_each(|(_, (_, local_status), (_, remote_status))| {
            local_status.1 = NetworkBandwitdth::Mbps(10.0);
            remote_status.1 = NetworkBandwitdth::Mbps(1.0);
        });

    println!();
    exec_similar(&replicas);

    eprintln!("time elapsed: {:.3?}", execution_time.elapsed());
}
