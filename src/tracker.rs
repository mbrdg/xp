pub trait Tracker {
    type Event;

    fn register(&mut self, event: Self::Event);
    fn finish(&mut self, false_matches: usize);
    fn events(&self) -> &Vec<Self::Event>;
    fn false_matches(&self) -> Option<usize>;
}

#[derive(Debug)]
pub enum NetworkHop {
    LocalToRemote(usize),
    RemoteToLocal(usize),
}

#[derive(Debug, Default)]
pub struct DefaultTracker {
    events: Vec<NetworkHop>,
    false_matches: Option<usize>,
}

impl DefaultTracker {
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            events: vec![],
            false_matches: None,
        }
    }
}

impl Tracker for DefaultTracker {
    type Event = NetworkHop;

    fn register(&mut self, event: Self::Event) {
        if let None = self.false_matches {
            self.events.push(event)
        }
    }

    fn finish(&mut self, false_matches: usize) {
        self.false_matches = Some(false_matches)
    }

    fn events(&self) -> &Vec<Self::Event> {
        &self.events
    }

    fn false_matches(&self) -> Option<usize> {
        self.false_matches
    }
}
