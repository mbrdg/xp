#![allow(dead_code)]

use std::{
    any, mem,
    ops::Range,
    time::{Duration, Instant},
};

use crate::{
    crdt::GSet,
    sync::Protocol,
    tracker::{DefaultTracker, NetworkBandwitdth, NetworkEvent, Tracker},
};

use rand::{
    distributions::{Alphanumeric, DistString, Distribution, Uniform},
    rngs::StdRng,
    SeedableRng,
};
use sync::{
    baseline::Baseline, bloombuckets::BloomBuckets, buckets::Buckets, Bloomer, BucketDispatcher,
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
    assert_eq!(
        gsets
            .0
            .elements()
            .symmetric_difference(gsets.1.elements())
            .count(),
        2 * num_diff_items
    );

    gsets
}

fn spawn_distinct_gset(
    cardinality: usize,
    lengths: Range<usize>,
    rng: &mut StdRng,
) -> GSet<String> {
    let mut replica = GSet::new();

    Uniform::from(lengths)
        .sample_iter(rng.clone())
        .map(|len| Alphanumeric.sample_string(rng, len))
        .take(cardinality)
        .for_each(|item| {
            replica.insert(item);
        });

    assert_eq!(cardinality, replica.len());
    replica
}

#[derive(Debug)]
struct ReplicaStatus {
    size: usize,
    bandwidth: NetworkBandwitdth,
}

/// Runs the specified protocol and outputs the metrics obtained.
fn run<P>(
    protocol: &mut P,
    id: Option<&str>,
    similarity: f64,
    local: &ReplicaStatus,
    remote: &ReplicaStatus,
) where
    P: Protocol<Tracker = DefaultTracker>,
{
    assert!(
        (0.0..=1.0).contains(&similarity),
        "similarity should be a ratio between 0.0 and 1.0"
    );

    let (size_of_local, upload) = (local.size, local.bandwidth);
    let (size_of_remote, download) = (remote.size, remote.bandwidth);

    let mut tracker = DefaultTracker::new(download, upload);
    protocol.sync(&mut tracker);

    let type_name = {
        let name = any::type_name_of_val(&protocol).split("::").last().unwrap();
        id.map_or(name.to_string(), |i| format!("{name}<{i}>"))
    };

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
        run(&mut protocol, None, *s, &local.1, &remote.1);
    });

    // Buckets<0.2>
    replicas.iter().for_each(|(s, local, remote)| {
        let dispatcher = BucketDispatcher::new((0.2 * local.0.len() as f64) as usize);
        let mut protocol = Buckets::with_dispatcher(local.0.clone(), remote.0.clone(), dispatcher);
        run(&mut protocol, Some("lf=0.2"), *s, &local.1, &remote.1);
    });

    // Buckets<1.0>
    // NOTE: A Bucket with a `load_factor` of 1.0 is the default.
    replicas.iter().for_each(|(s, local, remote)| {
        let mut protocol = Buckets::new(local.0.clone(), remote.0.clone());
        run(&mut protocol, Some("lf=1.0"), *s, &local.1, &remote.1);
    });

    // Buckets<5.0>
    replicas.iter().for_each(|(s, local, remote)| {
        let dispatcher = BucketDispatcher::new(5 * local.0.len());
        let mut protocol = Buckets::with_dispatcher(local.0.clone(), remote.0.clone(), dispatcher);
        run(&mut protocol, Some("lf=5.0"), *s, &local.1, &remote.1);
    });

    // BloomBuckets<1.0, 0.01>
    // NOTE: A bucket with load_factor` of 1.0 and a false positive rate of 1% are the defaults.
    replicas.iter().for_each(|(s, local, remote)| {
        let mut protocol = BloomBuckets::new(local.0.clone(), remote.0.clone());
        run(&mut protocol, Some("fpr=1%"), *s, &local.1, &remote.1);
    });

    // BloomBuckets<1.0, 0.025>
    replicas.iter().for_each(|(s, local, remote)| {
        let bloomer = Bloomer::new(0.05);
        let mut protocol = BloomBuckets::with_bloomer(local.0.clone(), remote.0.clone(), bloomer);
        run(&mut protocol, Some("fpr=5%"), *s, &local.1, &remote.1);
    });
}

type DistinctReplicas<T> = ((T, ReplicaStatus), (T, ReplicaStatus));
fn exec_distinct(replicas: &DistinctReplicas<GSet<String>>) {
    let (local, remote) = replicas;

    // Baseline
    let mut protocol = Baseline::new(local.0.clone(), remote.0.clone());
    run(&mut protocol, None, 0.0, &local.1, &remote.1);

    // Buckets<0.2>
    let dispatcher = BucketDispatcher::new((0.2 * local.0.len() as f64) as usize);
    let mut protocol = Buckets::with_dispatcher(local.0.clone(), remote.0.clone(), dispatcher);
    run(&mut protocol, Some("lf=0.2"), 0.0, &local.1, &remote.1);

    // Buckets<1.0>
    // NOTE: A Bucket with a `load_factor` of 1.0 is the default.
    let mut protocol = Buckets::new(local.0.clone(), remote.0.clone());
    run(&mut protocol, Some("lf=1.0"), 0.0, &local.1, &remote.1);

    // Buckets<5.0>
    let dispatcher = BucketDispatcher::new(5 * local.0.len());
    let mut protocol = Buckets::with_dispatcher(local.0.clone(), remote.0.clone(), dispatcher);
    run(&mut protocol, Some("lf=5.0"), 0.0, &local.1, &remote.1);

    // BloomBuckets<1.0, 0.01>
    // NOTE: A bucket with load_factor` of 1.0 and a false positive rate of 1% are the defaults.
    let mut protocol = BloomBuckets::new(local.0.clone(), remote.0.clone());
    run(&mut protocol, Some("fpr=1%"), 0.0, &local.1, &remote.1);

    // BloomBuckets<1.0, 0.05>
    let bloomer = Bloomer::new(0.05);
    let mut protocol = BloomBuckets::with_bloomer(local.0.clone(), remote.0.clone(), bloomer);
    run(&mut protocol, Some("fpr=5%"), 0.0, &local.1, &remote.1);
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

            let local_status = ReplicaStatus {
                size: local.elements().iter().map(String::len).sum(),
                bandwidth: NetworkBandwitdth::Mbps(10.0),
            };

            let remote_status = ReplicaStatus {
                size: remote.elements().iter().map(String::len).sum(),
                bandwidth: NetworkBandwitdth::Mbps(10.0),
            };

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
            local_status.bandwidth = NetworkBandwitdth::Mbps(1.0);
            remote_status.bandwidth = NetworkBandwitdth::Mbps(10.0);
        });

    println!();
    exec_similar(&replicas);

    // Upload >> Download
    replicas
        .iter_mut()
        .for_each(|(_, (_, local_status), (_, remote_status))| {
            local_status.bandwidth = NetworkBandwitdth::Mbps(10.0);
            remote_status.bandwidth = NetworkBandwitdth::Mbps(1.0);
        });

    println!();
    exec_similar(&replicas);

    // Second Experiment - Different cardinalities
    // |local| >> |remote|
    let mut replicas = {
        let local_replica = spawn_distinct_gset(100_000, 50..80, &mut rng);
        let local_status = ReplicaStatus {
            size: local_replica.elements().iter().map(String::len).sum(),
            bandwidth: NetworkBandwitdth::Mbps(10.0),
        };

        let remote_replica = spawn_distinct_gset(10_000, 50..80, &mut rng);
        let remote_status = ReplicaStatus {
            size: remote_replica.elements().iter().map(String::len).sum(),
            bandwidth: NetworkBandwitdth::Mbps(10.0),
        };

        (
            (local_replica, local_status),
            (remote_replica, remote_status),
        )
    };

    // Upload == Download
    println!();
    exec_distinct(&replicas);

    // Upload << Download
    replicas.0 .1.bandwidth = NetworkBandwitdth::Mbps(1.0);
    replicas.1 .1.bandwidth = NetworkBandwitdth::Mbps(10.0);

    println!();
    exec_distinct(&replicas);

    // Upload >> Download
    replicas.0 .1.bandwidth = NetworkBandwitdth::Mbps(10.0);
    replicas.1 .1.bandwidth = NetworkBandwitdth::Mbps(1.0);

    println!();
    exec_distinct(&replicas);

    // |local| << |remote|
    let (size_of_local, size_of_remote) = (replicas.0 .1.size, replicas.1 .1.size);
    mem::swap(&mut replicas.0, &mut replicas.1);
    assert_eq!(replicas.0 .1.size, size_of_remote);
    assert_eq!(replicas.1 .1.size, size_of_local);

    // Upload == Download
    replicas.0 .1.bandwidth = NetworkBandwitdth::Mbps(10.0);
    replicas.1 .1.bandwidth = NetworkBandwitdth::Mbps(10.0);

    println!();
    exec_distinct(&replicas);

    // Upload << Download
    replicas.0 .1.bandwidth = NetworkBandwitdth::Mbps(1.0);
    replicas.1 .1.bandwidth = NetworkBandwitdth::Mbps(10.0);

    println!();
    exec_distinct(&replicas);

    // Upload >> Download
    replicas.0 .1.bandwidth = NetworkBandwitdth::Mbps(10.0);
    replicas.1 .1.bandwidth = NetworkBandwitdth::Mbps(1.0);

    println!();
    exec_distinct(&replicas);

    eprintln!("time elapsed: {:.3?}", execution_time.elapsed());
}
