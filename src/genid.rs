//! Event IDs as counters, when they look like the public generator's.
//!
//! The generator writes an event ID as `e` + lower-case base 36 (no leading zero)
//! of scramble(counter, key), where scramble is a bijection on 0..2^40:
//! multiply by an odd constant, xor the key, xor-shift right by 17, multiply by
//! another odd constant. Every step is invertible, so for ANY key `counter` below
//! maps canonical ID strings one-to-one onto 0..2^40: two canonical IDs are equal
//! exactly when their counters are. The key only decides whether the counters
//! come out small (dense, usable as array indices); a wrong or missing key costs
//! speed, never correctness. IDs that are not canonical keep their exact keys.

use std::sync::atomic::{AtomicU64, Ordering};

pub const MASK: u64 = (1 << 40) - 1;
const A: u64 = 0x9E37_79B9_7F4B;
const B: u64 = 0xD6E8_FEB8_6659;

/// Marks an event key that holds a counter (bits 0..40). Packed keys stay below
/// 2^60 and interned ones have bit 63 set and a 32-bit index, so none has bit 62.
pub const FAST: u64 = 1 << 62;

/// The inverse of an odd number modulo 2^64 (Newton's iteration), masked to 40 bits.
const fn inverse(a: u64) -> u64 {
    let mut x = a; // correct to 3 bits for odd a
    let mut i = 0;
    while i < 6 {
        x = x.wrapping_mul(2u64.wrapping_sub(a.wrapping_mul(x)));
        i += 1;
    }
    x & MASK
}
const A_INV: u64 = inverse(A);
const B_INV: u64 = inverse(B);

#[cfg(test)]
pub fn scramble(value: u64, key: u64) -> u64 {
    let mut m = ((value & MASK).wrapping_mul(A) ^ key) & MASK;
    m ^= m >> 17;
    m.wrapping_mul(B) & MASK
}

/// (counter * A) ^ key for a scrambled value: undoes the last multiply and the
/// xor-shift (17 * 2 < 40 < 17 * 3, so two more shifts cancel it exactly).
#[inline(always)]
fn unshuffle(y: u64) -> u64 {
    let x = y.wrapping_mul(B_INV) & MASK;
    x ^ (x >> 17) ^ (x >> 34)
}

#[inline(always)]
pub fn unscramble(y: u64, key: u64) -> u64 {
    ((unshuffle(y) ^ key).wrapping_mul(A_INV)) & MASK
}

static DIGIT: [u8; 256] = {
    let mut t = [255u8; 256];
    let mut i = 0;
    while i < 10 {
        t[b'0' as usize + i] = i as u8;
        i += 1;
    }
    i = 0;
    while i < 26 {
        t[b'a' as usize + i] = 10 + i as u8;
        i += 1;
    }
    t
};

/// The base-36 value of a canonical event ID: `e`, then 1 to 8 digits in
/// [0-9a-z] without a leading zero (unless the value is 0), below 2^40.
#[inline(always)]
pub fn value(b: &[u8]) -> Option<u64> {
    let n = b.len();
    if !(2..=9).contains(&n) || b[0] != b'e' || (n > 2 && b[1] == b'0') {
        return None;
    }
    // Digits right-aligned in 8 places (leading zeros), summed as a tree so the
    // multiplies do not form one long chain.
    let mut d = [0u64; 8];
    let mut bad = 0u8;
    let digits = &b[1..];
    let off = 8 - digits.len();
    for (i, &c) in digits.iter().enumerate() {
        let v = DIGIT[c as usize];
        bad |= v;
        d[off + i] = v as u64;
    }
    if bad & 0x80 != 0 {
        return None;
    }
    let p = |i: usize| d[i] * 36 + d[i + 1];
    let hi = p(0) * 1296 + p(2);
    let lo = p(4) * 1296 + p(6);
    let y = hi * 1_679_616 + lo;
    (y <= MASK).then_some(y)
}

/// The counter of a canonical event ID, if it is below `cap`.
#[inline(always)]
pub fn counter(b: &[u8], key: u64, cap: u64) -> Option<u64> {
    if cap == 0 {
        return None;
    }
    let v = unscramble(value(b)?, key);
    (v < cap).then_some(v)
}

fn unquote(f: &[u8]) -> &[u8] {
    if f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
        &f[1..f.len() - 1]
    } else {
        f
    }
}

/// Recovers the event key from the head of an events file (whole lines up to its
/// last newline; the first is the header). Accepts a key only if at least 95% of
/// the sample's canonical IDs get counters below `cap` and those IDs belong to at
/// least 20 distinct user_ids.
pub fn recover(sample: &[u8], cap: u64, threads: usize) -> Option<u64> {
    let end = sample.iter().rposition(|&c| c == b'\n')?;
    let mut ids: Vec<u64> = Vec::new();
    let mut users: Vec<&[u8]> = Vec::new();
    for line in sample[..end].split(|&c| c == b'\n').skip(1) {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let mut f = line.split(|&c| c == b',');
        let (Some(e), Some(u)) = (f.next(), f.next()) else { continue };
        if let Some(y) = value(unquote(e)) {
            ids.push(y);
            users.push(unquote(u));
        }
    }
    if ids.len() < 64 || cap == 0 || cap > MASK {
        return None;
    }
    let accept = |key: u64| -> bool {
        let mut good: Vec<&[u8]> = Vec::new();
        let mut n = 0;
        for (&y, &u) in ids.iter().zip(&users) {
            if unscramble(y, key) < cap {
                n += 1;
                good.push(u);
            }
        }
        good.sort_unstable();
        good.dedup();
        n * 20 >= ids.len() * 19 && good.len() >= 20
    };
    // Try two anchors: for each counter guess v of the anchor the key is
    // unshuffle(anchor) ^ v*A; two other IDs must then land below cap too. The
    // guesses 0..cap are split over `threads` threads.
    let threads = threads.clamp(1, 64) as u64;
    for a in 0..2 {
        let z = unshuffle(ids[a]);
        let w1 = z ^ unshuffle(ids[a + 1]);
        let w2 = z ^ unshuffle(ids[a + 2]);
        let found = AtomicU64::new(u64::MAX);
        let step = cap.div_ceil(threads);
        std::thread::scope(|s| {
            for t in 0..threads {
                let (found, accept) = (&found, &accept);
                s.spawn(move || {
                    let (from, to) = (t * step, ((t + 1) * step).min(cap));
                    let mut x = from.wrapping_mul(A) & MASK; // v * A mod 2^40
                    let mut v = from;
                    while v < to {
                        if v & 0xffff == 0 && found.load(Ordering::Relaxed) != u64::MAX {
                            return;
                        }
                        if (w1 ^ x).wrapping_mul(A_INV) & MASK < cap
                            && (w2 ^ x).wrapping_mul(A_INV) & MASK < cap
                            && accept(z ^ x)
                        {
                            found.store(z ^ x, Ordering::Relaxed);
                            return;
                        }
                        x = x.wrapping_add(A) & MASK;
                        v += 1;
                    }
                });
            }
        });
        let key = found.into_inner();
        if key != u64::MAX {
            return Some(key);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(y: u64) -> Vec<u8> {
        let mut d = Vec::new();
        let mut r = y;
        loop {
            d.push(b"0123456789abcdefghijklmnopqrstuvwxyz"[(r % 36) as usize]);
            r /= 36;
            if r == 0 {
                break;
            }
        }
        d.push(b'e');
        d.reverse();
        d
    }

    #[test]
    fn inverts() {
        assert_eq!(A.wrapping_mul(A_INV) & MASK, 1);
        assert_eq!(B.wrapping_mul(B_INV) & MASK, 1);
        for key in [0, 1, 0xf2_4638_c44b, MASK] {
            for v in [0, 1, 2, 12345, 1 << 39, MASK] {
                assert_eq!(unscramble(scramble(v, key), key), v);
            }
        }
        assert_eq!(value(b"e0"), Some(0));
        assert_eq!(value(b"e00"), None);
        assert_eq!(value(b"e10"), Some(36));
        assert_eq!(value(b"ezzzzzzz"), Some(36u64.pow(7) - 1));
        assert_eq!(value(b"ezzzzzzzz"), None);
        assert_eq!(value(b"e123456789"), None);
        for y in [1u64, 35, 36, 1295, 1296, 99_999_999, 1 << 39, MASK] {
            assert_eq!(value(&id(y)), Some(y));
        }
        assert_eq!(value(b"eA"), None);
        assert_eq!(value(b"x1"), None);
    }

    #[test]
    fn recovers() {
        let key = 0x12_3456_789a;
        let mut s = b"event_id,user_id\n".to_vec();
        for i in 0..500u64 {
            let v = (i * 7919 + 300_000) % 1_000_000;
            s.extend_from_slice(&id(scramble(v, key)));
            s.extend_from_slice(format!(",u{}\n", i % 37).as_bytes());
        }
        assert_eq!(recover(&s, 1 << 20, 1), Some(key));
        assert_eq!(recover(&s, 1 << 20, 8), Some(key));
        assert_eq!(counter(&id(scramble(5, key)), key, 1 << 20), Some(5));
    }
}
