use std::{
    borrow::Borrow,
    cmp::max,
    f64::consts::LN_2,
    hash::{BuildHasher, Hash, RandomState},
    marker::PhantomData,
};

use anyhow::ensure;
use bitvec::{bitvec, slice::BitSlice, vec::BitVec};

#[derive(Clone, Debug, Default)]
pub struct BloomFilter<T: ?Sized> {
    arr: BitVec,
    hashers: [RandomState; 2],
    k: u64,
    marker: PhantomData<T>,
}

impl<T> BloomFilter<T>
where
    T: ?Sized,
{
    pub fn new(capacity: usize, epsilon: f64) -> anyhow::Result<Self> {
        ensure!(
            epsilon > 0.0,
            "false positive rate should be larger than 0.0, got {epsilon}"
        );
        ensure!(
            (0.0..1.0).contains(&epsilon),
            "false positive rate should be a value in the interval (0.0..1.0), got {epsilon}"
        );

        let m = (-1.0f64 * capacity as f64 * epsilon.ln() / (LN_2 * LN_2)).ceil() as usize;
        let k = (-1.0f64 * epsilon.ln() / LN_2).ceil() as u64;

        Ok(Self {
            arr: bitvec!(0; max(m, 1)),
            hashers: [RandomState::new(), RandomState::new()],
            k,
            marker: PhantomData,
        })
    }

    pub fn as_bitslice(&self) -> &BitSlice {
        &self.arr
    }
}

impl<T> BloomFilter<T>
where
    T: ?Sized + Hash,
{
    pub fn contains<Q: ?Sized + Hash>(&self, value: &Q) -> bool
    where
        T: Borrow<Q>,
    {
        let h = (
            self.hashers[0].hash_one(value),
            self.hashers[1].hash_one(value),
        );

        (0..self.k).all(|i| {
            let bit = h.0.wrapping_add(i.wrapping_mul(h.1)) as usize % self.arr.len();
            self.arr[bit]
        })
    }

    pub fn insert(&mut self, value: &T) {
        let h = (
            self.hashers[0].hash_one(value),
            self.hashers[1].hash_one(value),
        );

        for i in 0..self.k {
            let bit = h.0.wrapping_add(i.wrapping_mul(h.1)) as usize % self.arr.len();
            self.arr.set(bit, true);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::BloomFilter;

    #[test]
    fn membership_test() {
        let mut filter = BloomFilter::<&str>::new(100, 0.01).unwrap();

        assert!(!filter.contains(&"a"));
        assert!(!filter.contains(&"b"));

        filter.insert(&"a");
        assert!(filter.contains(&"a"));
    }
}
