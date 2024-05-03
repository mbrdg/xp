pub trait Tracker {
    type Event;

    fn register(&mut self, event: Self::Event);
    fn finish(&mut self, false_matches: usize);
    fn events(&self) -> &Vec<Self::Event>;
    fn diffs(&self) -> Option<usize>;
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
    diffs: Option<usize>,
}

impl DefaultTracker {
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            events: vec![],
            diffs: None,
        }
    }
}

impl Tracker for DefaultTracker {
    type Event = NetworkHop;

    fn register(&mut self, event: Self::Event) {
        if let None = self.diffs {
            self.events.push(event)
        }
    }

    fn finish(&mut self, diffs: usize) {
        self.diffs = Some(diffs)
    }

    fn events(&self) -> &Vec<Self::Event> {
        &self.events
    }

    fn diffs(&self) -> Option<usize> {
        self.diffs
    }
}
