use std::time::Duration;

pub trait EventTracker {
    type Event;

    fn register(&mut self, event: Self::Event);
    fn events(&self) -> &Vec<Self::Event>;
}

pub trait SyncTracker {
    fn freeze(&mut self, diffs: usize);
    fn diffs(&self) -> Result<usize, &str>;
}

pub trait NetworkTracker {
    fn download(&self) -> usize;
    fn upload(&self) -> usize;
}

#[derive(Debug)]
pub enum NetworkHop {
    LocalToRemote { bytes: usize, duration: Duration },
    RemoteToLocal { bytes: usize, duration: Duration },
}

impl NetworkHop {
    #[inline]
    #[must_use]
    pub fn as_local_to_remote(upload: usize, bytes: usize) -> Self {
        assert!(upload > 0, "bandwidth should be greater than 0");

        Self::LocalToRemote {
            bytes,
            duration: Duration::from_millis(u64::try_from(bytes * 1000 / upload).unwrap()),
        }
    }

    #[inline]
    #[must_use]
    pub fn as_remote_to_local(download: usize, bytes: usize) -> Self {
        assert!(download > 0, "bandwidth should be greater than 0");

        Self::RemoteToLocal {
            bytes,
            duration: Duration::from_millis(u64::try_from(bytes * 1000 / download).unwrap()),
        }
    }

    pub fn bytes(&self) -> usize {
        match self {
            Self::LocalToRemote { bytes, duration: _ } => *bytes,
            Self::RemoteToLocal { bytes, duration: _ } => *bytes,
        }
    }

    pub fn duration(&self) -> Duration {
        match self {
            Self::LocalToRemote { bytes: _, duration } => *duration,
            Self::RemoteToLocal { bytes: _, duration } => *duration,
        }
    }
}

#[derive(Debug, Default)]
pub struct DefaultTracker {
    events: Vec<NetworkHop>,
    diffs: Option<usize>,
    download: usize,
    upload: usize,
}

impl DefaultTracker {
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
}

impl EventTracker for DefaultTracker {
    type Event = NetworkHop;

    fn register(&mut self, event: NetworkHop) {
        if self.diffs.is_none() {
            self.events.push(event)
        }
    }

    fn events(&self) -> &Vec<NetworkHop> {
        &self.events
    }
}

impl SyncTracker for DefaultTracker {
    fn freeze(&mut self, diffs: usize) {
        if self.diffs.is_none() {
            self.diffs = Some(diffs)
        }
    }

    fn diffs(&self) -> Result<usize, &str> {
        self.diffs
            .ok_or("`freeze()` should be called before getting the diffs")
    }
}

impl NetworkTracker for DefaultTracker {
    fn download(&self) -> usize {
        self.download
    }

    fn upload(&self) -> usize {
        self.upload
    }
}
