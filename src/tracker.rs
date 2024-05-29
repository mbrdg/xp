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

#[derive(Debug)]
pub enum NetworkBandwitdth {
    KBits(usize),
    MBits(usize),
    GBits(usize),
    TBits(usize),
}

impl NetworkBandwitdth {
    #[inline]
    pub fn bytes_per_sec(&self) -> usize {
        match self {
            NetworkBandwitdth::KBits(b) => 125 * b,
            NetworkBandwitdth::MBits(b) => 125_000 * b,
            NetworkBandwitdth::GBits(b) => 125_000_000 * b,
            NetworkBandwitdth::TBits(b) => 125_000_000 * b,
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
    pub fn local_to_remote(upload: usize, bytes: usize) -> Self {
        assert!(upload > 0, "upload should be greater than 0");

        Self::LocalToRemote {
            bytes,
            duration: Duration::from_millis(u64::try_from(bytes * 1000 / upload).unwrap()),
        }
    }

    #[inline]
    #[must_use]
    pub fn remote_to_local(download: usize, bytes: usize) -> Self {
        assert!(download > 0, "download should be greater than 0");

        Self::RemoteToLocal {
            bytes,
            duration: Duration::from_millis(u64::try_from(bytes * 1000 / download).unwrap()),
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
    download: usize,
    upload: usize,
}

impl<E> DefaultTracker<E>
where
    E: Event,
{
    #[inline]
    #[must_use]
    pub fn new(download: usize, upload: usize) -> Self {
        assert!(download > 0, "download should be greater than 0");
        assert!(upload > 0, "upload should be greater than 0");

        Self {
            events: vec![],
            diffs: None,
            download,
            upload,
        }
    }

    #[inline]
    pub fn download(&self) -> usize {
        self.download
    }

    #[inline]
    pub fn upload(&self) -> usize {
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
