use std::{
    f64::consts::LN_2,
    hash::{BuildHasher, Hash, RandomState},
    marker::PhantomData,
};

use bitvec::{bitvec, vec::BitVec};

pub struct BloomFilter<T: ?Sized> {
    base: BitVec,
    hashers: [RandomState; 2],
    k: u64,
    marker: PhantomData<T>,
}

impl<T> BloomFilter<T>
where
    T: ?Sized + Hash,
{
    #[inline]
    #[must_use]
    pub fn with_capacity_and_rate(capacity: usize, rate: f64) -> Self {
        assert!(
            0.0 <= rate && rate <= 1.0,
            "rate should be in the interval [0.0, 1.0)"
        );

        // Compute the optimal bitarray size `m` and the optimal number of hash functions `k`
        let m = (-1.0f64 * capacity as f64 * rate.ln() / (LN_2 * LN_2)).ceil() as usize;
        let k = (-1.0f64 * rate.ln() / LN_2).ceil() as u64;

        Self {
            base: bitvec![0; m],
            hashers: [RandomState::new(), RandomState::new()],
            k,
            marker: PhantomData,
        }
    }

    #[inline]
    pub fn contains(&self, value: &T) -> bool {
        let h = (
            self.hashers[0].hash_one(value),
            self.hashers[1].hash_one(value),
        );

        (0..self.k).all(|i| {
            let bit =
                usize::try_from(h.0.wrapping_add(i.wrapping_mul(h.1))).unwrap() % self.base.len();
            self.base[bit]
        })
    }

    #[inline]
    pub fn insert(&mut self, value: &T) {
        let h = (
            self.hashers[0].hash_one(value),
            self.hashers[1].hash_one(value),
        );

        (0..self.k).for_each(|i| {
            let bit =
                usize::try_from(h.0.wrapping_add(i.wrapping_mul(h.1))).unwrap() % self.base.len();
            self.base.set(bit, true);
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_membership() {
        let mut bloom = BloomFilter::with_capacity_and_rate(100, 0.01);

        assert!(!bloom.contains("1"));
        assert!(!bloom.contains("2"));

        bloom.insert("1");
        assert!(bloom.contains("1"));
    }
}
