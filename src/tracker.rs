#[derive(Debug)]
pub enum TrackerError {
    FinishedTracker,
}

pub trait Tracker {
    type Event;

    fn register(&mut self, event: Self::Event) -> Result<(), TrackerError>;
    fn finish(&mut self, false_matches: usize);
    fn events(&self) -> &Vec<Self::Event>;
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

    fn register(&mut self, event: Self::Event) -> Result<(), TrackerError> {
        match self.false_matches {
            Some(_) => Err(TrackerError::FinishedTracker),
            None => {
                self.events.push(event);
                Ok(())
            }
        }
    }

    fn finish(&mut self, false_matches: usize) {
        self.false_matches = Some(false_matches)
    }

    fn events(&self) -> &Vec<Self::Event> {
        &self.events
    }
}
