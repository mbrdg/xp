pub mod gcounter;
pub mod gset;

pub trait Decomposable {
    type Decomposition: Sized;

    fn split(&self) -> Vec<Self::Decomposition>;
    fn join(&mut self, deltas: Vec<Self::Decomposition>);
    fn difference(&self, remote: &Self::Decomposition) -> Self::Decomposition;
}
