//! Order-preserving f64 ↔ u64 encoding for zset score-index keys.
//! Standard sign-flip trick: memcmp order of the encoded u64 equals numeric
//! order of the f64 (with -0.0 == 0.0 normalized and NaN rejected upstream).

#[inline]
pub fn encode_score(f: f64) -> u64 {
    let f = if f == 0.0 { 0.0 } else { f }; // normalize -0.0
    let bits = f.to_bits();
    if bits >> 63 == 1 {
        !bits
    } else {
        bits ^ (1 << 63)
    }
}

#[inline]
pub fn decode_score(u: u64) -> f64 {
    let bits = if u >> 63 == 1 { u ^ (1 << 63) } else { !u };
    f64::from_bits(bits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordering_matches() {
        let vals = [
            f64::NEG_INFINITY,
            -1e300,
            -2.5,
            -1.0,
            -f64::MIN_POSITIVE,
            0.0,
            f64::MIN_POSITIVE,
            0.5,
            1.0,
            2.5,
            1e300,
            f64::INFINITY,
        ];
        for w in vals.windows(2) {
            assert!(
                encode_score(w[0]) < encode_score(w[1]),
                "{} !< {}",
                w[0],
                w[1]
            );
        }
    }

    #[test]
    fn roundtrip() {
        for f in [-123.456, 0.0, 1.0, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(decode_score(encode_score(f)), f);
        }
        // -0.0 normalizes to 0.0
        assert_eq!(decode_score(encode_score(-0.0)), 0.0);
    }
}
