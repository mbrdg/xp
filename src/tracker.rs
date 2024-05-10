use std::time::Duration;

pub trait Tracker {
    type Event;

    fn register(&mut self, event: Self::Event);
    fn finish(&mut self, differences: usize);
    fn events(&self) -> &Vec<(Self::Event, Duration)>;
    fn differences(&self) -> Option<usize>;
}

#[derive(Debug)]
pub enum NetworkHop {
    LocalToRemote(usize),
    RemoteToLocal(usize),
}

impl NetworkHop {
    #[inline]
    pub fn bytes(&self) -> usize {
        match self {
            Self::LocalToRemote(bytes) => *bytes,
            Self::RemoteToLocal(bytes) => *bytes,
        }
    }
}

#[derive(Debug, Default)]
pub struct DefaultTracker {
    events: Vec<(NetworkHop, Duration)>,
    differences: Option<usize>,
    baudrate: usize,
}

impl DefaultTracker {
    #[inline]
    #[must_use]
    pub fn new(bytes_per_sec: usize) -> Self {
        assert_ne!(bytes_per_sec, 0);

        Self {
            events: vec![],
            differences: None,
            baudrate: bytes_per_sec,
        }
    }
}

impl Tracker for DefaultTracker {
    type Event = NetworkHop;

    fn register(&mut self, event: Self::Event) {
        if let None = self.differences {
            let duration = Duration::from_secs_f64(event.bytes() as f64 / self.baudrate as f64);
            self.events.push((event, duration))
        }
    }

    fn finish(&mut self, differences: usize) {
        if let None = self.differences {
            self.differences = Some(differences)
        }
    }

    fn events(&self) -> &Vec<(NetworkHop, Duration)> {
        &self.events
    }

    fn differences(&self) -> Option<usize> {
        self.differences
    }
}
