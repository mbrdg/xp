#![allow(dead_code)]

use std::{
    any,
    ops::Range,
    time::{Duration, Instant},
    usize,
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
use sync::{baseline::Baseline, buckets::Buckets, BucketDispatcher};

mod bloom;
mod crdt;
mod sync;
mod tracker;

fn populate_gset(cardinality: usize, lengths: Range<usize>, rng: &mut StdRng) -> GSet<String> {
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

fn populate_similar_gsets(
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

    let ReplicaStatus {
        size: size_of_local,
        bandwidth: upload,
    } = local;
    let ReplicaStatus {
        size: size_of_remote,
        bandwidth: download,
    } = remote;

    let mut tracker = DefaultTracker::new(*download, *upload);
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

fn main() {
    let execution_time = Instant::now();

    let seed = rand::random();
    let mut rng = StdRng::seed_from_u64(seed);
    println!("seed={seed}");

    // First experiment - symmetric channels and similar cardinality
    let (download, upload) = (NetworkBandwitdth::Mbps(10.0), NetworkBandwitdth::Mbps(10.0));
    let replicas = (0..=100)
        .rev()
        .step_by(10)
        .map(|s| {
            let cardinality = 100_000;
            let lengths = 50..80;
            let s = f64::from(s) / 100.0;

            let (local, remote) = populate_similar_gsets(cardinality, lengths, s, &mut rng);

            let local_status = ReplicaStatus {
                size: local.elements().iter().map(String::len).sum(),
                bandwidth: download,
            };
            let remote_status = ReplicaStatus {
                size: remote.elements().iter().map(String::len).sum(),
                bandwidth: upload,
            };

            (s, (local, local_status), (remote, remote_status))
        })
        .collect::<Vec<_>>();

    // Baseline
    replicas.iter().for_each(|(s, local, remote)| {
        let mut protocol = Baseline::new(local.0.clone(), remote.0.clone());
        run(&mut protocol, None, *s, &local.1, &remote.1);
    });

    // Buckets<0.5>
    replicas.iter().for_each(|(s, local, remote)| {
        let dispatcher = BucketDispatcher::new((0.5 * local.0.len() as f64) as usize);
        let mut protocol = Buckets::with_dispatcher(local.0.clone(), remote.0.clone(), dispatcher);
        run(&mut protocol, Some("lf=0.5"), *s, &local.1, &remote.1);
    });

    // Buckets<1.0>
    // NOTE: A Bucket with a `loading_factor` of 1.0 is the default.
    replicas.iter().for_each(|(s, local, remote)| {
        let mut protocol = Buckets::new(local.0.clone(), remote.0.clone());
        run(&mut protocol, Some("lf=1.0"), *s, &local.1, &remote.1);
    });

    // Buckets<4.0>
    replicas.iter().for_each(|(s, local, remote)| {
        let dispatcher = BucketDispatcher::new((4.0 * local.0.len() as f64) as usize);
        let mut protocol = Buckets::with_dispatcher(local.0.clone(), remote.0.clone(), dispatcher);
        run(&mut protocol, Some("lf=4.0"), *s, &local.1, &remote.1);
    });

    eprintln!("time elapsed: {:.3?}", execution_time.elapsed());
}
