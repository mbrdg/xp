use fxhash::FxHashMap;
use std::{cmp::max, collections::BTreeSet, hash::Hash};

/// A `Dot` is a simple struct that uniquely identifies operations issued by replicas, i.e., it is
/// a pair (replica id, sequence number).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Dot<I>(pub I, pub u64);

/// A Dot context is a causality tracking mechanism. It is made of two compoents: a clock and a
/// cloud. The clock encodes causality sequentially in a compressed manner, i.e., no gaps allowed.
/// On the other hand, the cloud is a set of dots that permit gaps in causality.
///
/// # Example
///
/// ```
/// use crdt::{Dot, DotContext};
///
/// let mut ctx = DotContext::new();
/// assert_eq!(ctx.next(&"a"), 1);
/// assert_eq!(ctx.next(&"b"), 1);
/// assert_eq!(ctx.next(&"a"), 2);
///
/// assert_eq!(ctx.contains(&Dot("a", 1)), true);
/// assert_eq!(ctx.contains(&Dot("b", 1)), true);
/// assert_eq!(ctx.contains(&Dot("a", 2)), true);
/// assert_eq!(ctx.contains(&Dot("b", 2)), false);
/// ```
#[derive(Clone, Debug, Default)]
pub struct DotContext<I> {
    clock: FxHashMap<I, u64>,
    cloud: BTreeSet<Dot<I>>,
}

impl<I> DotContext<I> {
    /// Creates an empty `DotContext`.
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            clock: FxHashMap::default(),
            cloud: BTreeSet::new(),
        }
    }
}

impl<I> DotContext<I>
where
    I: Eq + Hash + Ord,
{
    /// Returns `true` if `dot` is contained by the causal context `self`, i.e.,
    /// `dot` is already coverred by the vector clock or `dot` is present in the detached set of
    /// dots.
    pub fn contains(&self, dot: &Dot<I>) -> bool {
        let Dot(id, seq) = dot;
        self.clock.get(id).is_some_and(|clock| clock >= seq) || self.cloud.contains(dot)
    }

    /// Compacts the representation of the dot context.
    ///
    /// The algorithm iterates in sorted order thorugh the cloud of dots and determines if each dot
    /// is either already present in the clock or it is increments the clock. If either of these is
    /// the case the dot is moved from the cloud to the clock, otherwise it remains intact.
    pub fn compact(&mut self) {
        self.cloud
            .retain(|Dot(id, seq)| match self.clock.get_mut(id) {
                Some(clock) if *clock == *seq - 1 => {
                    *clock += 1;
                    false
                }
                Some(clock) if *clock >= *seq => false,
                _ => true,
            })
    }
}

impl<I> DotContext<I>
where
    I: Clone + Eq + Hash,
{
    /// Increments and returns the next timestamp for replica represented by `id`. If the entry is
    /// not found then it creates a new entry and returns 1.
    pub fn next(&mut self, id: &I) -> u64 {
        match self.clock.get_mut(id) {
            Some(clock) => {
                *clock += 1;
                *clock
            }
            None => {
                self.clock.insert(id.clone(), 1);
                1
            }
        }
    }
}

impl<I> DotContext<I>
where
    I: Eq + Hash + Ord + Clone,
{
    /// Joins together `self` with `other`, i.e., `self` becomes up to date with the contents of
    /// `other`.
    ///
    /// The algorithm updates the clock with the missing or ahead entries from `other`.
    /// Then the new cloud of dots results from the union between the dot clouds of `self` and
    /// `other`. Finally, context compaction is done.
    pub fn join(&mut self, other: &Self) {
        other
            .clock
            .iter()
            .for_each(|(id, remote_clock)| match self.clock.get_mut(id) {
                Some(local_clock) => *local_clock = max(*local_clock, *remote_clock),
                None => {
                    self.clock.insert(id.clone(), *remote_clock);
                }
            });

        let unknown_dots = other
            .cloud
            .difference(&self.cloud)
            .cloned()
            .collect::<Vec<_>>();
        self.cloud.extend(unknown_dots);

        self.compact()
    }
}

impl<I> PartialEq for DotContext<I>
where
    I: Hash + Eq + Ord,
{
    fn eq(&self, other: &Self) -> bool {
        self.clock == other.clock && self.cloud == other.cloud
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use fxhash::FxHashMap;

    use crate::{Dot, DotContext};

    #[test]
    fn test_membership() {
        let ctx = DotContext {
            clock: FxHashMap::from_iter([("a", 3), ("b", 6)]),
            cloud: BTreeSet::from([Dot("a", 4), Dot("a", 6), Dot("b", 9), Dot("c", 3)]),
        };

        assert_eq!(ctx.contains(&Dot("a", 2)), true);
        assert_eq!(ctx.contains(&Dot("a", 3)), true);
        assert_eq!(ctx.contains(&Dot("a", 4)), true);
        assert_eq!(ctx.contains(&Dot("a", 5)), false);

        assert_eq!(ctx.contains(&Dot("b", 6)), true);
        assert_eq!(ctx.contains(&Dot("b", 7)), false);
        assert_eq!(ctx.contains(&Dot("b", 8)), false);
        assert_eq!(ctx.contains(&Dot("b", 9)), true);
        assert_eq!(ctx.contains(&Dot("b", 10)), false);

        assert_eq!(ctx.contains(&Dot("c", 2)), false);
        assert_eq!(ctx.contains(&Dot("c", 3)), true);
        assert_eq!(ctx.contains(&Dot("c", 4)), false);

        assert_eq!(ctx.contains(&Dot("d", 2)), false);
    }

    #[test]
    fn test_next() {
        let mut ctx = DotContext {
            clock: FxHashMap::from_iter([("a", 3), ("b", 6)]),
            cloud: BTreeSet::from([Dot("a", 4), Dot("a", 6), Dot("b", 9), Dot("c", 3)]),
        };

        assert_eq!(ctx.next(&"a"), 4);
        assert_eq!(ctx.next(&"a"), 5);
        assert_eq!(ctx.next(&"b"), 7);
        assert_eq!(ctx.next(&"c"), 1);
        assert_eq!(ctx.next(&"d"), 1);
    }

    #[test]
    fn test_compactation() {
        let mut ctx = DotContext {
            clock: FxHashMap::from_iter([("a", 3), ("b", 6)]),
            cloud: BTreeSet::from([Dot("a", 4), Dot("a", 5), Dot("b", 9), Dot("c", 3)]),
        };

        let expected_ctx = DotContext {
            clock: FxHashMap::from_iter([("a", 5), ("b", 6)]),
            cloud: BTreeSet::from([Dot("b", 9), Dot("c", 3)]),
        };

        ctx.compact();
        assert_eq!(ctx, expected_ctx);
    }

    #[test]
    fn test_joining() {
        let mut local = DotContext {
            clock: FxHashMap::from_iter([("a", 3), ("b", 6), ("c", 4), ("d", 2)]),
            cloud: BTreeSet::from([Dot("a", 11), Dot("b", 10), Dot("c", 3)]),
        };

        let remote = DotContext {
            clock: FxHashMap::from_iter([("a", 9), ("b", 10), ("d", 2), ("e", 2)]),
            cloud: BTreeSet::from([Dot("d", 3), Dot("e", 4)]),
        };

        let expected_local_ctx = DotContext {
            clock: FxHashMap::from_iter([("a", 9), ("b", 10), ("c", 4), ("d", 3), ("e", 2)]),
            cloud: BTreeSet::from([Dot("a", 11), Dot("e", 4)]),
        };

        local.join(&remote);
        assert_eq!(local, expected_local_ctx);
    }
}
