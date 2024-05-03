pub trait Tracker {
    type Event;

    fn register(&mut self, event: Self::Event);
    fn finish(&mut self, differences: usize);
    fn events(&self) -> &Vec<Self::Event>;
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
            Self::LocalToRemote(b) => *b,
            Self::RemoteToLocal(b) => *b,
        }
    }
}

#[derive(Debug, Default)]
pub struct DefaultTracker {
    events: Vec<NetworkHop>,
    differences: Option<usize>,
}

impl DefaultTracker {
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            events: vec![],
            differences: None,
        }
    }
}

impl Tracker for DefaultTracker {
    type Event = NetworkHop;

    fn register(&mut self, event: Self::Event) {
        if let None = self.differences {
            self.events.push(event)
        }
    }

    fn finish(&mut self, diffs: usize) {
        self.differences = Some(diffs)
    }

    fn events(&self) -> &Vec<Self::Event> {
        &self.events
    }

    fn differences(&self) -> Option<usize> {
        self.differences
    }
}
