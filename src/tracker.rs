use std::time::Duration;

pub trait Event {}
pub trait Tracker {
    type Event: Event;

    fn register(&mut self, event: Self::Event);
    fn events(&self) -> &Vec<Self::Event>;
    fn finish(&mut self, diffs: usize);
    fn diffs(&self) -> Option<usize>;
    fn is_synced(&self) -> bool;
}

/// Network Bandwidth value in bits per second.
#[derive(Clone, Copy, Debug)]
pub enum NetworkBandwitdth {
    Kbps(f64),
    Mbps(f64),
    Gbps(f64),
}

impl Default for NetworkBandwitdth {
    fn default() -> Self {
        Self::Kbps(0.0)
    }
}

impl NetworkBandwitdth {
    #[inline]
    pub fn as_bytes_per_sec(&self) -> f64 {
        match self {
            NetworkBandwitdth::Kbps(b) => b / 8.0 * 1_000.0,
            NetworkBandwitdth::Mbps(b) => b / 8.0 * 1_000_000.0,
            NetworkBandwitdth::Gbps(b) => b / 8.0 * 1_000_000_000.0,
        }
    }
}

#[derive(Debug)]
pub enum NetworkEvent {
    LocalToRemote { bytes: usize, duration: Duration },
    RemoteToLocal { bytes: usize, duration: Duration },
}

impl NetworkEvent {
    #[inline]
    #[must_use]
    pub fn local_to_remote(upload: NetworkBandwitdth, bytes: usize) -> Self {
        let bandwidth = upload.as_bytes_per_sec();
        assert!(
            bandwidth > 0.0,
            "upload should be greater than 0 bytes per second"
        );

        Self::LocalToRemote {
            bytes,
            duration: Duration::from_secs_f64(bytes as f64 / bandwidth),
        }
    }

    #[inline]
    #[must_use]
    pub fn remote_to_local(download: NetworkBandwitdth, bytes: usize) -> Self {
        let bandwidth = download.as_bytes_per_sec();
        assert!(
            bandwidth > 0.0,
            "download should be greater than 0 bytes per second"
        );

        Self::RemoteToLocal {
            bytes,
            duration: Duration::from_secs_f64(bytes as f64 / bandwidth),
        }
    }

    #[inline]
    pub fn bytes(&self) -> usize {
        match self {
            Self::LocalToRemote { bytes, duration: _ } => *bytes,
            Self::RemoteToLocal { bytes, duration: _ } => *bytes,
        }
    }

    #[inline]
    pub fn duration(&self) -> Duration {
        match self {
            Self::LocalToRemote { bytes: _, duration } => *duration,
            Self::RemoteToLocal { bytes: _, duration } => *duration,
        }
    }
}

impl Event for NetworkEvent {}

#[derive(Debug, Default)]
pub struct DefaultTracker<E = NetworkEvent> {
    events: Vec<E>,
    diffs: Option<usize>,
    download: NetworkBandwitdth,
    upload: NetworkBandwitdth,
}

impl<E> DefaultTracker<E>
where
    E: Event,
{
    #[inline]
    #[must_use]
    pub fn new(download: NetworkBandwitdth, upload: NetworkBandwitdth) -> Self {
        assert!(
            download.as_bytes_per_sec() > 0.0,
            "download should be greater than 0 bytes per second"
        );
        assert!(
            upload.as_bytes_per_sec() > 0.0,
            "upload should be greater than 0 bytes per second"
        );

        Self {
            events: vec![],
            diffs: None,
            download,
            upload,
        }
    }

    #[inline]
    pub fn download(&self) -> NetworkBandwitdth {
        self.download
    }

    #[inline]
    pub fn upload(&self) -> NetworkBandwitdth {
        self.upload
    }
}

impl<E> Tracker for DefaultTracker<E>
where
    E: Event,
{
    type Event = E;

    fn register(&mut self, event: Self::Event) {
        if self.diffs.is_none() {
            self.events.push(event);
        }
    }

    fn events(&self) -> &Vec<Self::Event> {
        &self.events
    }

    fn finish(&mut self, diffs: usize) {
        if self.diffs.is_none() {
            self.diffs = Some(diffs)
        }
    }

    fn diffs(&self) -> Option<usize> {
        self.diffs
    }

    fn is_synced(&self) -> bool {
        self.diffs.is_some_and(|d| d == 0)
    }
}
