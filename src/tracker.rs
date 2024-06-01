use std::time::Duration;

pub trait Event {}
pub trait Tracker {
    type Event: Event;

    fn is_ready(&self) -> bool;
    fn register(&mut self, event: Self::Event);
    fn events(&self) -> &Vec<Self::Event>;
    fn finish(&mut self, diffs: usize);
    fn diffs(&self) -> usize;
}

/// Network Bandwidth value in bits per second.
#[derive(Clone, Copy, Debug)]
pub enum NetworkBandwitdth {
    Kbps(f64),
    Mbps(f64),
    Gbps(f64),
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

/// Type of Event used by the [`DefaultTracker`].
/// It holds the size of the transfered payload in Bytes and estimates the duration based on the
/// bandwidth provided by the tracker that registers these kind of events.
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
            Self::LocalToRemote { bytes, .. } => *bytes,
            Self::RemoteToLocal { bytes, .. } => *bytes,
        }
    }

    #[inline]
    pub fn duration(&self) -> Duration {
        match self {
            Self::LocalToRemote { duration, .. } => *duration,
            Self::RemoteToLocal { duration, .. } => *duration,
        }
    }
}

impl Event for NetworkEvent {}

/// Default [`Tracker`] for operations over the Network.
#[derive(Debug)]
pub struct DefaultTracker {
    events: Vec<NetworkEvent>,
    diffs: Option<usize>,
    download: NetworkBandwitdth,
    upload: NetworkBandwitdth,
}

impl DefaultTracker {
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

impl Tracker for DefaultTracker {
    type Event = NetworkEvent;

    fn is_ready(&self) -> bool {
        self.events.is_empty() && self.diffs.is_none()
    }

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

    fn diffs(&self) -> usize {
        self.diffs
            .expect("`finish()` should be called before `diffs()`")
    }
}
