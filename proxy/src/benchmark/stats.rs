//! Statistics helpers for the source benchmark: nearest-rank percentiles over
//! latency deltas. Adapted from geyserbench's stats core, with the standard
//! (ascending) percentile convention — NOT shred-stats' inverted descending
//! sort.

/// Percentile over an ascending-sorted slice using the interpolated-rank
/// (round(p*(n-1))) index — the same convention geyserbench uses. `p` in
/// `[0.0, 1.0]`. Returns 0 for an empty slice.
pub fn percentile_sorted(sorted_ascending: &[i64], p: f64) -> i64 {
    if sorted_ascending.is_empty() {
        return 0;
    }
    let n = sorted_ascending.len();
    let p = p.clamp(0.0, 1.0);
    let idx = (p * (n as f64 - 1.0)).round() as usize;
    sorted_ascending[idx.min(n - 1)]
}

/// Sort `samples` ascending in place and return the percentiles in `ps`.
pub fn quantiles(samples: &mut [i64], ps: &[f64]) -> Vec<i64> {
    samples.sort_unstable();
    ps.iter().map(|&p| percentile_sorted(samples, p)).collect()
}

/// Mean of a slice as i64 (rounded to nearest), 0 for empty.
pub fn mean(samples: &[i64]) -> i64 {
    if samples.is_empty() {
        return 0;
    }
    let sum: i128 = samples.iter().map(|&x| x as i128).sum();
    (sum as f64 / samples.len() as f64).round() as i64
}

/// A ratio expressed in basis points (1/10000), clamped to `[0, 10000]`.
pub fn basis_points(numerator: u64, denominator: u64) -> i64 {
    if denominator == 0 {
        return 0;
    }
    ((numerator as u128 * 10_000) / denominator as u128).min(10_000) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_basic() {
        let mut v = vec![5, 1, 3, 2, 4];
        let q = quantiles(&mut v, &[0.0, 0.5, 1.0]);
        assert_eq!(q, vec![1, 3, 5]);
    }

    #[test]
    fn percentile_empty() {
        assert_eq!(percentile_sorted(&[], 0.5), 0);
        let mut empty: Vec<i64> = vec![];
        assert_eq!(quantiles(&mut empty, &[0.5]), vec![0]);
    }

    #[test]
    fn basis_points_works() {
        assert_eq!(basis_points(1, 2), 5000);
        assert_eq!(basis_points(0, 0), 0);
        assert_eq!(basis_points(5, 5), 10_000);
        assert_eq!(basis_points(10, 5), 10_000); // clamped
    }

    #[test]
    fn mean_works() {
        assert_eq!(mean(&[2, 4, 6]), 4);
        assert_eq!(mean(&[]), 0);
        assert_eq!(mean(&[-10, 10]), 0);
    }
}
