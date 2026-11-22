//! Shopping Sessions engine (Rust). Implements rules.md exactly; v1 DuckDB pipeline (git history) is the reference.
use memchr::{memchr, memchr_iter};
use memmap2::Mmap;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::{self, File};
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::time::Instant;
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

type S = &'static [u8];
const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const CHUNK: usize = 16 << 20;
const ORGANIC: u32 = u32::MAX;

#[derive(Default)]
struct XH(u64);
impl Hasher for XH {
    fn write(&mut self, b: &[u8]) {
        self.0 = xxh3_64_with_seed(b, self.0);
    }
    fn write_usize(&mut self, _: usize) {}
    fn finish(&self) -> u64 {
        self.0
    }
}
type Map<K, V> = HashMap<K, V, BuildHasherDefault<XH>>;

fn log(t0: &Instant, m: &str) {
    eprintln!("[{:7.2}s] {}", t0.elapsed().as_secs_f64(), m);
}

// ---------------- CSV dialect ----------------
#[inline]
fn field(f: S) -> Result<Option<S>, ()> {
    if memchr(b'"', f).is_some() {
        let n = f.len();
        if n >= 2 && f[0] == b'"' && f[n - 1] == b'"' && memchr(b'"', &f[1..n - 1]).is_none() {
            let v = &f[1..n - 1];
            Ok(if v.is_empty() { None } else { Some(v) })
        } else {
            Err(())
        }
    } else {
        Ok(if f.is_empty() { None } else { Some(f) })
    }
}

#[inline(always)]
fn fin(f: S, quotes: u32) -> Result<Option<S>, ()> {
    if quotes == 0 {
        Ok(if f.is_empty() { None } else { Some(f) })
    } else {
        let n = f.len();
        if quotes == 2 && n >= 2 && f[0] == b'"' && f[n - 1] == b'"' {
            Ok(if n == 2 { None } else { Some(&f[1..n - 1]) })
        } else {
            Err(())
        }
    }
}

/// Scalar split: single pass counting quotes per field.
#[inline]
fn split_scalar(line: S, out: &mut [Option<S>]) -> bool {
    let n = out.len();
    let mut k = 0;
    let mut start = 0;
    let mut q = 0u32;
    for (i, &c) in line.iter().enumerate() {
        if c == b',' {
            if k >= n { return false; }
            match fin(&line[start..i], q) {
                Ok(v) => out[k] = v,
                Err(_) => return false,
            }
            k += 1;
            start = i + 1;
            q = 0;
        } else if c == b'"' {
            q += 1;
        }
    }
    if k >= n { return false; }
    match fin(&line[start..], q) {
        Ok(v) => out[k] = v,
        Err(_) => return false,
    }
    k + 1 == n
}

#[inline(always)]
fn populate_out_const<const NF: usize>(
    data: S,
    line_start: usize,
    commas: &[u32; 16],
    line_end: usize,
    out: &mut [Option<S>],
) {
    if NF == 7 {
        let c0 = commas[0] as usize;
        let c1 = commas[1] as usize;
        let c2 = commas[2] as usize;
        let c3 = commas[3] as usize;
        let c4 = commas[4] as usize;
        let c5 = commas[5] as usize;
        out[0] = if line_start == c0 { None } else { Some(&data[line_start..c0]) };
        out[1] = if c0 + 1 == c1 { None } else { Some(&data[c0 + 1..c1]) };
        out[2] = if c1 + 1 == c2 { None } else { Some(&data[c1 + 1..c2]) };
        out[3] = if c2 + 1 == c3 { None } else { Some(&data[c2 + 1..c3]) };
        out[4] = if c3 + 1 == c4 { None } else { Some(&data[c3 + 1..c4]) };
        out[5] = if c4 + 1 == c5 { None } else { Some(&data[c4 + 1..c5]) };
        out[6] = if c5 + 1 == line_end { None } else { Some(&data[c5 + 1..line_end]) };
    } else if NF == 4 {
        let c0 = commas[0] as usize;
        let c1 = commas[1] as usize;
        let c2 = commas[2] as usize;
        out[0] = if line_start == c0 { None } else { Some(&data[line_start..c0]) };
        out[1] = if c0 + 1 == c1 { None } else { Some(&data[c0 + 1..c1]) };
        out[2] = if c1 + 1 == c2 { None } else { Some(&data[c1 + 1..c2]) };
        out[3] = if c2 + 1 == line_end { None } else { Some(&data[c2 + 1..line_end]) };
    } else if NF == 2 {
        let c0 = commas[0] as usize;
        out[0] = if line_start == c0 { None } else { Some(&data[line_start..c0]) };
        out[1] = if c0 + 1 == line_end { None } else { Some(&data[c0 + 1..line_end]) };
    } else {
        let mut prev = line_start;
        for k in 0..NF - 1 {
            let cp = commas[k] as usize;
            out[k] = if prev == cp { None } else { Some(&data[prev..cp]) };
            prev = cp + 1;
        }
        out[NF - 1] = if prev == line_end { None } else { Some(&data[prev..line_end]) };
    }
}

/// AVX-512 structural scan: 64-byte blocks, masks for \n, ',', '"'.
/// Lines without quotes populate `out` directly from comma mask positions.
/// Lines with quotes take the exact slow path `split_scalar`.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f", enable = "avx512bw")]
unsafe fn simd_for_lines<const NF: usize>(
    data: S,
    skip_header: bool,
    out: &mut [Option<S>],
    mut handler: impl FnMut(S, &mut [Option<S>], bool),
) {
    use std::arch::x86_64::*;

    let len = data.len();
    let ptr = data.as_ptr();

    let nl_v = _mm512_set1_epi8(b'\n' as i8);
    let cm_v = _mm512_set1_epi8(b',' as i8);
    let qt_v = _mm512_set1_epi8(b'"' as i8);

    let mut line_start: usize = 0;
    let mut skipping = skip_header;

    let mut has_quotes = false;
    let mut commas = [0u32; 16];
    let mut ncommas: usize = 0;

    let mut pos: usize = 0;

    while pos + 64 <= len {
        let chunk = _mm512_loadu_si512(ptr.add(pos) as *const _);
        let nl_mask = _mm512_cmpeq_epi8_mask(chunk, nl_v);
        let cm_mask = _mm512_cmpeq_epi8_mask(chunk, cm_v);
        let qt_mask = _mm512_cmpeq_epi8_mask(chunk, qt_v);

        if nl_mask == 0 {
            if qt_mask != 0 {
                has_quotes = true;
            }
            if !skipping && !has_quotes {
                let mut m = cm_mask;
                while m != 0 {
                    let bit = m.trailing_zeros();
                    if ncommas < 16 {
                        commas[ncommas] = (pos + bit as usize) as u32;
                    }
                    ncommas += 1;
                    m &= m - 1;
                }
            }
            pos += 64;
            continue;
        }

        let mut cur_nl = nl_mask;
        let mut cur_cm = cm_mask;
        let mut cur_qt = qt_mask;

        while cur_nl != 0 {
            let nl_bit = cur_nl.trailing_zeros();
            let nl_pos = pos + nl_bit as usize;

            let before_nl = (1u64 << nl_bit) - 1;
            let seg_cm = cur_cm & before_nl;
            let seg_qt = cur_qt & before_nl;

            if seg_qt != 0 {
                has_quotes = true;
            }

            if skipping {
                skipping = false;
            } else {
                let line = &data[line_start..nl_pos];
                if has_quotes {
                    if split_scalar(line, out) {
                        handler(line, out, true);
                    }
                } else {
                    let mut m = seg_cm;
                    while m != 0 {
                        let bit = m.trailing_zeros();
                        if ncommas < 16 {
                            commas[ncommas] = (pos + bit as usize) as u32;
                        }
                        ncommas += 1;
                        m &= m - 1;
                    }
                    if ncommas + 1 == NF {
                        populate_out_const::<NF>(data, line_start, &commas, nl_pos, out);
                        handler(line, out, false);
                    }
                }
            }

            line_start = nl_pos + 1;
            has_quotes = false;
            ncommas = 0;

            let clear_mask = if nl_bit < 63 { !((1u64 << (nl_bit + 1)) - 1) } else { 0 };
            cur_cm &= clear_mask;
            cur_qt &= clear_mask;
            cur_nl &= cur_nl - 1;
        }

        if cur_qt != 0 {
            has_quotes = true;
        }
        if !skipping && !has_quotes {
            let mut m = cur_cm;
            while m != 0 {
                let bit = m.trailing_zeros();
                if ncommas < 16 {
                    commas[ncommas] = (pos + bit as usize) as u32;
                }
                ncommas += 1;
                m &= m - 1;
            }
        }

        pos += 64;
    }

    // Scalar fallback for tail < 64 bytes
    while pos < len {
        let c = data[pos];
        if c == b'\n' {
            if skipping {
                skipping = false;
            } else {
                let line = &data[line_start..pos];
                if has_quotes {
                    if split_scalar(line, out) {
                        handler(line, out, true);
                    }
                } else if ncommas + 1 == NF {
                    populate_out_const::<NF>(data, line_start, &commas, pos, out);
                    handler(line, out, false);
                }
            }
            line_start = pos + 1;
            has_quotes = false;
            ncommas = 0;
        } else if c == b'"' {
            has_quotes = true;
        } else if c == b',' && !skipping && !has_quotes {
            if ncommas < 16 {
                commas[ncommas] = pos as u32;
            }
            ncommas += 1;
        }
        pos += 1;
    }

    if line_start < len && !skipping {
        let line = &data[line_start..];
        if has_quotes {
            if split_scalar(line, out) {
                handler(line, out, true);
            }
        } else if ncommas + 1 == NF {
            populate_out_const::<NF>(data, line_start, &commas, len, out);
            handler(line, out, false);
        }
    }
}

#[inline(always)]
fn for_lines_split<const NF: usize>(
    d: S,
    skip_header: bool,
    out: &mut [Option<S>],
    handler: impl FnMut(S, &mut [Option<S>], bool),
) {
    #[cfg(target_arch = "x86_64")]
    {
        #[cfg(target_feature = "avx512f")]
        {
            unsafe { simd_for_lines::<NF>(d, skip_header, out, handler); }
            return;
        }
        #[cfg(not(target_feature = "avx512f"))]
        {
            if is_x86_feature_detected!("avx512f") && is_x86_feature_detected!("avx512bw") {
                unsafe { simd_for_lines::<NF>(d, skip_header, out, handler); }
                return;
            }
        }
    }
    for_lines_split_scalar(d, skip_header, NF, out, handler);
}

fn for_lines_split_scalar(
    d: S,
    skip_header: bool,
    _nfields: usize,
    out: &mut [Option<S>],
    mut handler: impl FnMut(S, &mut [Option<S>], bool),
) {
    let mut start = 0;
    let mut skip = skip_header;
    for p in memchr_iter(b'\n', d) {
        if skip {
            skip = false;
        } else {
            let line = &d[start..p];
            if split_scalar(line, out) {
                let has_q = line.iter().any(|&c| c == b'"');
                handler(line, out, has_q);
            }
        }
        start = p + 1;
    }
    if start < d.len() && !skip {
        let line = &d[start..];
        if split_scalar(line, out) {
            let has_q = line.iter().any(|&c| c == b'"');
            handler(line, out, has_q);
        }
    }
}

// Keep the old for_lines for simple 2-field tables (products, campaigns)
fn for_lines(d: S, skip_header: bool, mut f: impl FnMut(S)) {
    let mut start = 0;
    let mut skip = skip_header;
    for p in memchr_iter(b'\n', d) {
        if skip {
            skip = false;
        } else {
            f(&d[start..p]);
        }
        start = p + 1;
    }
    if start < d.len() && !skip {
        f(&d[start..]);
    }
}

#[inline]
fn split(line: S, out: &mut [Option<S>]) -> bool {
    split_scalar(line, out)
}

/// Memory-map every *.csv in dir and cut it into ~CHUNK pieces at line boundaries. bool = piece starts the file.
fn chunks(dir: &Path) -> Vec<(S, bool)> {
    let mut v = Vec::new();
    let Ok(rd) = fs::read_dir(dir) else { return v };
    let mut paths: Vec<PathBuf> = rd.filter_map(|e| e.ok().map(|e| e.path())).collect();
    paths.sort();
    for p in paths {
        if p.extension().map(|e| e == "csv").unwrap_or(false) && p.is_file() {
            let d = map_file(&p);
            let mut pos = 0;
            let mut first = true;
            while pos < d.len() {
                let mut end = (pos + CHUNK).min(d.len());
                if end < d.len() {
                    end = match memchr(b'\n', &d[end..]) {
                        Some(q) => end + q + 1,
                        None => d.len(),
                    };
                }
                v.push((&d[pos..end], first));
                first = false;
                pos = end;
            }
        }
    }
    v
}

fn map_file(p: &Path) -> S {
    let f = File::open(p).expect("open");
    if f.metadata().map(|m| m.len()).unwrap_or(0) == 0 {
        return &[];
    }
    let m = unsafe { Mmap::map(&f).expect("mmap") };
    #[cfg(unix)]
    {
        let _ = m.advise(memmap2::Advice::WillNeed);
    }
    let m: &'static Mmap = Box::leak(Box::new(m));
    &m[..]
}

// ---------------- timestamps ----------------
#[inline]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    let (a, b) = (s[i], s[i + 1]);
    if a.is_ascii_digit() && b.is_ascii_digit() {
        Some(((a - b'0') as i64) * 10 + (b - b'0') as i64)
    } else {
        None
    }
}

const DB_Y0: i64 = 1900;
const DB_N: i64 = 300;
/// days_from_civil(y, m, 1) for y in DB_Y0..DB_Y0+DB_N, built at compile time
const DB: [i32; (DB_N * 12) as usize] = {
    let mut t = [0i32; (DB_N * 12) as usize];
    let mut i = 0;
    while i < (DB_N * 12) as usize {
        t[i] = days_from_civil(DB_Y0 + (i / 12) as i64, (i % 12) as i64 + 1, 1) as i32;
        i += 1;
    }
    t
};

const fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline(always)]
fn hash16(lo: u64, hi: u64) -> u64 {
    let mut z = lo ^ hi.rotate_left(32);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

fn parse_ts(s: S) -> Option<i64> {
    let n = s.len();
    // ISO fast path only when s[4] is '-': an all-digit F3 value with leading zeros can also be 20 or 25 long
    if (n == 20 || n == 25) && s[4] == b'-' {
        if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
            return None;
        }
        // all 14 date/time digits checked in one branch-free pass
        let dg = |i: usize| s[i].wrapping_sub(b'0');
        let ds = [dg(0), dg(1), dg(2), dg(3), dg(5), dg(6), dg(8), dg(9), dg(11), dg(12), dg(14), dg(15), dg(17), dg(18)];
        if ds.iter().fold(0u8, |m, &d| m | (d > 9) as u8) != 0 {
            return None;
        }
        let v = |k: usize| ds[k] as i64;
        let y = v(0) * 1000 + v(1) * 100 + v(2) * 10 + v(3);
        let (mo, d, h, mi, se) = (v(4) * 10 + v(5), v(6) * 10 + v(7), v(8) * 10 + v(9), v(10) * 10 + v(11), v(12) * 10 + v(13));
        let off = if n == 20 {
            if s[19] != b'Z' {
                return None;
            }
            0
        } else {
            let sg = match s[19] {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            if s[22] != b':' {
                return None;
            }
            let (oh, om) = (d2(s, 20)?, d2(s, 23)?);
            if oh > 23 || om > 59 {
                return None;
            }
            sg * (oh * 3_600_000 + om * 60_000)
        };
        let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
        let dim = match mo {
            2 => if leap { 29 } else { 28 },
            4 | 6 | 9 | 11 => 30,
            1..=12 => 31,
            _ => return None,
        };
        if d < 1 || d > dim || h > 23 || mi > 59 || se > 59 {
            return None;
        }
        let days = if (DB_Y0..DB_Y0 + DB_N).contains(&y) {
            DB[((y - DB_Y0) * 12 + mo - 1) as usize] as i64 + d - 1
        } else {
            days_from_civil(y, mo, d)
        };
        Some(days * DAY + h * 3_600_000 + mi * 60_000 + se * 1000 - off)
    } else {
        // F3: digits only; leading zeros allowed (strip them first, then <= 19 significant digits)
        if n == 0 || s.iter().any(|c| !c.is_ascii_digit()) {
            return None;
        }
        let mut i = 0;
        while i < n && s[i] == b'0' {
            i += 1;
        }
        let t = &s[i..];
        if t.len() > 19 {
            return None;
        }
        let mut v: u64 = 0;
        for &c in t {
            v = v * 10 + (c - b'0') as u64;
        }
        if v > i64::MAX as u64 { None } else { Some(v as i64) }
    }
}

fn parse_price(s: Option<S>) -> i64 {
    // digits only, any number of leading zeros ("0250" is 250), value 1..=10,000,000
    let Some(s) = s else { return -1 };
    let mut v: i64 = 0;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 || v > 10_000_000 {
            return -1; // non-digit, or already over the max (bad either way)
        }
        v = v * 10 + d as i64;
    }
    if (1..=10_000_000).contains(&v) { v } else { -1 }
}

// ---------------- records ----------------
#[derive(Clone, Copy)]
struct EvKey {
    h: u64,
    ingest: i64,
    loc: u64,
}

#[derive(Clone, Copy)]
struct EventPayload {
    id: S,
    rec: EvRec,
}

#[derive(Clone, Copy, Default)]
struct EvRec {
    key: u64,
    cat_price: u64,
}

impl EvRec {
    #[inline(always)]
    fn is_valid(&self) -> bool {
        (self.cat_price >> 32) as u32 != INVALID
    }
}

#[derive(Clone, Copy)]
struct CkRec {
    key: u64,
    camp: u32,
    id: S,
}

const INVALID: u32 = u32::MAX - 1;
const NB1: usize = 256;
const SH1: u32 = 64 - 8;
const NB2: usize = 256;
const SH2: u32 = 64 - 8;

#[derive(Clone, Copy, Default)]
struct Slot {
    lo: u64,
    hi: u64,
    pid: u32,
    len: u32,
}
/// user id -> pid. Ids up to 16 bytes are compared inline (no pointer chase, no memcmp).
/// (lo, hi, len) identifies the id exactly: bytes [0..8] and [len-8..len] for len >= 8, else packed bytes.
struct PTab {
    mask: usize,
    slots: Vec<Slot>,
    long: Map<S, u32>,
}
/// exact byte-slice equality without a libc memcmp call for the common short ids (<= 16 bytes)
#[inline(always)]
fn eqs(a: S, b: S) -> bool {
    a.len() == b.len() && if a.len() <= 16 { key16(a) == key16(b) } else { a == b }
}
#[inline(always)]
fn key16(s: S) -> (u64, u64) {
    let n = s.len();
    if n >= 8 {
        (u64::from_le_bytes(s[..8].try_into().unwrap()), u64::from_le_bytes(s[n - 8..].try_into().unwrap()))
    } else {
        let mut x = 0u64;
        for (i, &c) in s.iter().enumerate() {
            x |= (c as u64) << (8 * i);
        }
        (x, 0)
    }
}
/// Exact compact user key (lo, hi, tag); tag == 0 means "cold" (only the byte slice identifies it).
/// tier 1: 1..=16 bytes -> key16 words, tag = len (1..=16)
/// tier 2: 33 bytes = any prefix byte + 32 lowercase hex -> 128-bit decode, tag = 33 | prefix << 8 | 1 << 16
/// Tags of the two tiers never coincide and each tier's encoding is injective, so (lo, hi, tag) equality
/// is exactly byte equality of the ids.
#[inline(always)]
fn ukey(s: S) -> (u64, u64, u32) {
    let n = s.len();
    if n.wrapping_sub(1) < 16 {
        let (a, b) = key16(s);
        return (a, b, n as u32);
    }
    if n == 33 {
        let h: &[u8; 32] = s[1..33].try_into().unwrap();
        let (mut a, mut b, mut bad) = (0u64, 0u64, 0u8);
        for i in 0..16 {
            let (d, v) = hexv(h[i]);
            bad |= d;
            a = (a << 4) | v;
        }
        for i in 16..32 {
            let (d, v) = hexv(h[i]);
            bad |= d;
            b = (b << 4) | v;
        }
        if bad == 0 {
            return (a, b, 33 | (s[0] as u32) << 8 | 1 << 16);
        }
    }
    (0, 0, 0)
}
/// lowercase hex digit -> (0, value); anything else -> (1, _)
#[inline(always)]
fn hexv(c: u8) -> (u8, u64) {
    let d = c.wrapping_sub(b'0');
    let l = c.wrapping_sub(b'a');
    if d < 10 {
        (0, d as u64)
    } else if l < 6 {
        (0, l as u64 + 10)
    } else {
        (1, 0)
    }
}
#[inline(always)]
fn uhash(lo: u64, hi: u64, tag: u32) -> u64 {
    hash16(lo, hi ^ ((tag as u64) << 40))
}
impl PTab {
    fn insert(&mut self, id: S, pid: u32) {
        let (lo, hi, tag) = ukey(id);
        if tag == 0 {
            self.long.insert(id, pid);
            return;
        }
        let mut i = (uhash(lo, hi, tag) as usize) & self.mask;
        loop {
            let sl = &mut self.slots[i];
            if sl.len == 0 {
                *sl = Slot { lo, hi, pid, len: tag };
                return;
            }
            if sl.len == tag && sl.lo == lo && sl.hi == hi {
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get(&self, u: S) -> Option<u32> {
        let (lo, hi, tag) = ukey(u);
        if tag == 0 {
            return self.long.get(u).copied();
        }
        let mut i = (uhash(lo, hi, tag) as usize) & self.mask;
        loop {
            let sl = &self.slots[i];
            if sl.len == 0 {
                return None;
            }
            if sl.len == tag && sl.lo == lo && sl.hi == hi {
                return Some(sl.pid);
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get_precomputed(&self, u: S, h: u64, lo: u64, hi: u64, tag: u32) -> Option<u32> {
        if tag == 0 {
            return self.long.get(u).copied();
        }
        let mut i = (h as usize) & self.mask;
        loop {
            let sl = unsafe { self.slots.get_unchecked(i) };
            if sl.len == 0 {
                return None;
            }
            if sl.len == tag && sl.lo == lo && sl.hi == hi {
                return Some(sl.pid);
            }
            i = (i + 1) & self.mask;
        }
    }
}

// payload fields are atomics (Relaxed), published by the Release store of `state` (sound, no aliasing UB)
struct InternSlot {
    lo: std::sync::atomic::AtomicU64,
    hi: std::sync::atomic::AtomicU64,
    len: AtomicU32,
    pid: AtomicU32,
    ptr: std::sync::atomic::AtomicPtr<u8>,
    state: std::sync::atomic::AtomicU32, // 0 = empty, 1 = claiming, 2 = ready
}
unsafe impl Send for InternSlot {}
unsafe impl Sync for InternSlot {}

struct Interner {
    mask: usize,
    slots: Vec<InternSlot>,
    next_pid: std::sync::atomic::AtomicU32,
    fallback: std::sync::Mutex<Map<S, u32>>,
}

impl Interner {
    fn new(cap: usize, start_pid: u32) -> Self {
        let mut slots = Vec::with_capacity(cap);
        slots.resize_with(cap, || InternSlot {
            lo: std::sync::atomic::AtomicU64::new(0),
            hi: std::sync::atomic::AtomicU64::new(0),
            len: AtomicU32::new(0),
            pid: AtomicU32::new(0),
            ptr: std::sync::atomic::AtomicPtr::new(std::ptr::null_mut()),
            state: std::sync::atomic::AtomicU32::new(0),
        });
        Self {
            mask: cap - 1,
            slots,
            next_pid: std::sync::atomic::AtomicU32::new(start_pid),
            fallback: std::sync::Mutex::new(Map::default()),
        }
    }

    #[inline(always)]
    fn intern(&self, u: S) -> u32 {
        let (lo, hi, tag) = ukey(u);
        self.intern_precomputed(u, uhash(lo, hi, tag), lo, hi, tag)
    }

    #[inline(always)]
    fn intern_precomputed(&self, u: S, h: u64, lo: u64, hi: u64, tag: u32) -> u32 {
        if tag == 0 {
            let mut fb = self.fallback.lock().unwrap();
            let next = &self.next_pid;
            return *fb.entry(u).or_insert_with(|| next.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
        }
        let len = tag;
        let mut i = (h as usize) & self.mask;
        let mut probes = 0;
        loop {
            let sl = unsafe { self.slots.get_unchecked(i) };
            let st = sl.state.load(std::sync::atomic::Ordering::Acquire);
            if st == 0 {
                if sl.state.compare_exchange(
                    0, 1,
                    std::sync::atomic::Ordering::Acquire,
                    std::sync::atomic::Ordering::Relaxed
                ).is_ok() {
                    let pid = self.next_pid.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let r = std::sync::atomic::Ordering::Relaxed;
                    sl.lo.store(lo, r);
                    sl.hi.store(hi, r);
                    sl.len.store(len, r);
                    sl.pid.store(pid, r);
                    sl.ptr.store(u.as_ptr() as *mut u8, r);
                    sl.state.store(2, std::sync::atomic::Ordering::Release);
                    return pid;
                }
                continue;
            } else if st == 1 {
                while sl.state.load(std::sync::atomic::Ordering::Acquire) == 1 {
                    std::hint::spin_loop();
                }
            }
            let r = std::sync::atomic::Ordering::Relaxed;
            if sl.len.load(r) == len && sl.lo.load(r) == lo && sl.hi.load(r) == hi {
                return sl.pid.load(r);
            }
            probes += 1;
            if probes > 128 {
                let mut fb = self.fallback.lock().unwrap();
                let next = &self.next_pid;
                return *fb.entry(u).or_insert_with(|| next.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
            }
            i = (i + 1) & self.mask;
        }
    }
}

#[inline(always)]
fn resolve_precomputed(ptab: &PTab, interner: &Interner, u: S, h: u64, lo: u64, hi: u64, tag: u32) -> u32 {
    if let Some(pid) = ptab.get_precomputed(u, h, lo, hi, tag) {
        pid
    } else {
        interner.intern_precomputed(u, h, lo, hi, tag)
    }
}

#[inline(always)]
fn resolve_user(ptab: &PTab, interner: &Interner, root_names: &[S], u: S) -> (u32, S) {
    if let Some(pid) = ptab.get(u) {
        (pid, root_names[pid as usize])
    } else {
        let pid = interner.intern(u);
        (pid, u)
    }
}

fn build_names(interner: &Interner, root_names: &[S]) -> Vec<S> {
    let total_pids = interner.next_pid.load(std::sync::atomic::Ordering::Relaxed) as usize;
    // keys pack pid into 24 bits ((pid << 40) | ts offset): fail loudly instead of merging people
    assert!(total_pids < (1usize << 24), "too many persons for the 24-bit pid key: {}", total_pids);
    let mut names: Vec<S> = vec![&[]; total_pids];
    for pid in 0..root_names.len() {
        names[pid] = root_names[pid];
    }
    for sl in &interner.slots {
        if sl.state.load(std::sync::atomic::Ordering::Acquire) == 2 {
            let r = std::sync::atomic::Ordering::Relaxed;
            let u = unsafe { std::slice::from_raw_parts(sl.ptr.load(r) as *const u8, (sl.len.load(r) & 0xFF) as usize) };
            names[sl.pid.load(r) as usize] = u;
        }
    }
    let fb = interner.fallback.lock().unwrap();
    for (&k, &pid) in fb.iter() {
        names[pid as usize] = k;
    }
    names
}

struct Ck {
    h: u64,
    id: S,
    user: Option<S>,
    camp: Option<S>,
    ts: Option<S>,
}

fn parse_two(file: &Path) -> Vec<(S, S)> {
    let d = map_file(file);
    let mut v = Vec::new();
    let mut f = [None; 2];
    for_lines(d, true, |l| {
        if split(l, &mut f) {
            if let (Some(a), Some(b)) = (f[0], f[1]) {
                v.push((a, b));
            }
        }
    });
    v
}

fn manifest_field(txt: &str, key: &str) -> String {
    let k = format!("\"{}\"", key);
    let i = txt.find(&k).expect("manifest key") + k.len();
    let rest = &txt[i..];
    let a = rest.find('"').unwrap() + 1;
    let b = a + rest[a..].find('"').unwrap();
    rest[a..b].to_string()
}

fn utf8(s: &[u8]) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(s)
}

fn main() {
    let t0 = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let ncpu: usize = std::env::var("NCPU").ok().and_then(|v| v.parse().ok()).unwrap_or(8);
    rayon::ThreadPoolBuilder::new().num_threads(ncpu.max(1)).build_global().ok();

    let man = fs::read_to_string(data.join("manifest.json")).expect("manifest");
    let ws = parse_ts(Box::leak(manifest_field(&man, "window_start").into_bytes().into_boxed_slice())).unwrap();
    let we = parse_ts(Box::leak(manifest_field(&man, "window_end").into_bytes().into_boxed_slice())).unwrap();

    let ws_day = ws.div_euclid(DAY) as i32;
    let we_day = (we - 1).div_euclid(DAY) as i32;
    let ndays = (we_day - ws_day + 1).max(1) as usize;
    // small tables
    let prods = parse_two(&data.join("products.csv"));
    let camps = parse_two(&data.join("campaigns.csv"));
    let mut cat_names: Vec<S> = Vec::new();
    let mut cat_id: Map<S, u32> = Map::default();
    let mut prod_cat: Map<S, u32> = Map::default();
    for &(p, c) in &prods {
        let id = *cat_id.entry(c).or_insert_with(|| {
            cat_names.push(c);
            (cat_names.len() - 1) as u32
        });
        prod_cat.insert(p, id);
    }
    let unknown: u32 = *cat_id.entry(b"unknown").or_insert_with(|| {
        cat_names.push(b"unknown");
        (cat_names.len() - 1) as u32
    });
    // rank of each category in byte order (for top_category ties)
    let mut order: Vec<u32> = (0..cat_names.len() as u32).collect();
    order.sort_by(|&a, &b| cat_names[a as usize].cmp(cat_names[b as usize]));
    let mut cat_rank = vec![0u32; cat_names.len()];
    for (r, &c) in order.iter().enumerate() {
        cat_rank[c as usize] = r as u32;
    }
    let mut camp_id: Map<S, u32> = Map::default();
    let mut camp_names: Vec<(S, S)> = Vec::new();
    for &(c, ch) in &camps {
        camp_id.entry(c).or_insert_with(|| {
            camp_names.push((c, ch));
            (camp_names.len() - 1) as u32
        });
    }
    let ncamp = camp_names.len();

    // links -> union-find; root = byte-wise smallest id
    let lchunks = chunks(&data.join("links"));
    let pairs: Vec<(S, S)> = lchunks
        .par_iter()
        .flat_map_iter(|&(d, first)| {
            let mut v = Vec::new();
            let mut f = [None; 2];
            for_lines(d, first, |l| {
                if split(l, &mut f) {
                    if let (Some(a), Some(b)) = (f[0], f[1]) {
                        v.push((a, b));
                    }
                }
            });
            v
        })
        .collect();
    let mut idx: Map<S, u32> = Map::default();
    let mut names: Vec<S> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    fn find(p: &mut [u32], mut x: u32) -> u32 {
        let mut r = x;
        while p[r as usize] != r {
            r = p[r as usize];
        }
        while p[x as usize] != r {
            let n = p[x as usize];
            p[x as usize] = r;
            x = n;
        }
        r
    }
    for &(a, b) in &pairs {
        let ia = *idx.entry(a).or_insert_with(|| {
            names.push(a);
            parent.push(names.len() as u32 - 1);
            names.len() as u32 - 1
        });
        let ib = *idx.entry(b).or_insert_with(|| {
            names.push(b);
            parent.push(names.len() as u32 - 1);
            names.len() as u32 - 1
        });
        let (ra, rb) = (find(&mut parent, ia), find(&mut parent, ib));
        if ra != rb {
            if names[ra as usize] < names[rb as usize] {
                parent[rb as usize] = ra;
            } else {
                parent[ra as usize] = rb;
            }
        }
    }
    let mut root_pid = vec![u32::MAX; names.len()];
    let mut root_names: Vec<S> = Vec::new();
    for i in 0..names.len() as u32 {
        let r = find(&mut parent, i);
        if root_pid[r as usize] == u32::MAX {
            root_pid[r as usize] = root_names.len() as u32;
            root_names.push(names[r as usize]);
        }
    }
    let num_roots = root_names.len() as u32;

    let mut cap = 1usize;
    while cap < names.len() * 2 + 16 {
        cap <<= 1;
    }
    let mut ptab = PTab { mask: cap - 1, slots: vec![Slot::default(); cap], long: Map::default() };
    for i in 0..names.len() as u32 {
        let r = find(&mut parent, i);
        let pid = root_pid[r as usize];
        ptab.insert(names[i as usize], pid);
    }
    log(&t0, &format!("links: {} pairs, {} ids, {} roots", pairs.len(), names.len(), num_roots));
    let ptab = &ptab;

    let echunks = chunks(&data.join("events"));
    let cchunks = chunks(&data.join("clicks"));
    let total_input_bytes: usize = echunks.iter().map(|c| c.0.len()).sum::<usize>()
        + cchunks.iter().map(|c| c.0.len()).sum::<usize>();
    let icap = if total_input_bytes < 10 << 20 {
        65_536
    } else if total_input_bytes < 200 << 20 {
        1_048_576
    } else {
        8_388_608
    };
    let interner = Interner::new(icap, num_roots);
    let interner_ref = &interner;
    let root_names_ref = &root_names;

    let base_ms = ws - WEEK;
    assert!(we - base_ms < (1i64 << 40), "window exceeds 40 bits");

    // ---------------- events || clicks (independent until the per-person pass) ----------------
    let (ev_pb, cl_pb) = rayon::join(
        || {
            const EVENT_BATCH: usize = 32;

            #[derive(Clone, Copy)]
            struct EventBatchItem {
                id: S,
                h: u64,
                ingest: i64,
                user: S,
                user_h: u64,
                lo: u64,
                hi: u64,
                tag: u32,
                ts: i64,
                cat: u32,
                price: i64,
                valid: bool,
            }

            const DUMMY_EVENT: EventBatchItem = EventBatchItem {
                id: &[],
                h: 0,
                ingest: 0,
                user: &[],
                user_h: 0,
                lo: 0,
                hi: 0,
                tag: 0,
                ts: 0,
                cat: INVALID,
                price: 0,
                valid: false,
            };

            let (payloads, keys_per_chunk): (Vec<Vec<EventPayload>>, Vec<Vec<Vec<EvKey>>>) = echunks
                .par_iter()
                .enumerate()
                .map(|(ci, &(d, first))| {
                    let mut payload: Vec<EventPayload> = Vec::with_capacity(d.len() / 70 + 16);
                    let cap = d.len() / 70 / NB1 + 16;
                    let mut out: Vec<Vec<EvKey>> = (0..NB1).map(|_| Vec::with_capacity(cap)).collect();
                    let mut f = [None; 7];
                    let mut batch = [DUMMY_EVENT; EVENT_BATCH];
                    let mut batch_len = 0;

                    let mut flush_events = |batch: &[EventBatchItem; EVENT_BATCH], len: usize, payload: &mut Vec<EventPayload>, out: &mut Vec<Vec<EvKey>>| {
                        if len == 0 { return; }
                        for item in &batch[..len] {
                            if item.valid && item.tag != 0 {
                                let slot_ptab = (item.user_h as usize) & ptab.mask;
                                let slot_intern = (item.user_h as usize) & interner_ref.mask;
                                unsafe {
                                    use core::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
                                    let p1 = ptab.slots.as_ptr().add(slot_ptab) as *const i8;
                                    _mm_prefetch(p1, _MM_HINT_T0);
                                    let p2 = interner_ref.slots.as_ptr().add(slot_intern) as *const i8;
                                    _mm_prefetch(p2, _MM_HINT_T0);
                                }
                            }
                        }

                        for item in &batch[..len] {
                            let rec = if item.valid {
                                let pid = resolve_precomputed(ptab, interner_ref, item.user, item.user_h, item.lo, item.hi, item.tag);
                                let ts_rel = (item.ts - base_ms) as u64;
                                let key = ((pid as u64) << 40) | ts_rel;
                                let cat_price = ((item.cat as u64) << 32) | (item.price as u32 as u64);
                                EvRec { key, cat_price }
                            } else {
                                EvRec { key: 0, cat_price: (INVALID as u64) << 32 }
                            };
                            let row = payload.len() as u32;
                            payload.push(EventPayload { id: item.id, rec });
                            let loc = ((ci as u64) << 32) | (row as u64);
                            out[(item.h >> SH1) as usize].push(EvKey { h: item.h, ingest: item.ingest, loc });
                        }
                    };

                    for_lines_split::<7>(d, first, &mut f, |_l, f, _q| {
                        if let Some(id) = f[0] {
                            if id.is_empty() { return; }
                            let h = xxh3_64(id);
                            let ingest = f[3].and_then(parse_ts).unwrap_or(i64::MAX);

                            let mut valid = false;
                            let mut user: S = &[];
                            let mut user_h = 0u64;
                            let mut lo = 0u64;
                            let mut hi = 0u64;
                            let mut tag = 0u32;
                            let mut ts = 0i64;
                            let mut cat = INVALID;
                            let mut price = 0i64;

                            if let (Some(u), Some(t_str), Some(e_str), Some(prod)) = (f[1], f[2], f[4], f[5]) {
                                if !u.is_empty() && !prod.is_empty() {
                                    if let Some(t) = parse_ts(t_str) {
                                        if t >= ws && t < we && ingest - t <= DAY {
                                            let et = if eqs(e_str, b"view") {
                                                1
                                            } else if eqs(e_str, b"cart") {
                                                2
                                            } else if eqs(e_str, b"purchase") {
                                                3
                                            } else {
                                                0
                                            };
                                            if et != 0 {
                                                let pr = if et == 3 { parse_price(f[6]) } else { 0 };
                                                if et != 3 || pr >= 1 {
                                                    user = u;
                                                    let (l, h_val, tg) = ukey(u);
                                                    lo = l;
                                                    hi = h_val;
                                                    tag = tg;
                                                    user_h = uhash(lo, hi, tg);
                                                    ts = t;
                                                    price = pr;
                                                    cat = if et == 3 { *prod_cat.get(prod).unwrap_or(&unknown) } else { u32::MAX };
                                                    valid = true;
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            batch[batch_len] = EventBatchItem {
                                id,
                                h,
                                ingest,
                                user,
                                user_h,
                                lo,
                                hi,
                                tag,
                                ts,
                                cat,
                                price,
                                valid,
                            };
                            batch_len += 1;
                            if batch_len == EVENT_BATCH {
                                flush_events(&batch, batch_len, &mut payload, &mut out);
                                batch_len = 0;
                            }
                        }
                    });
                    if batch_len > 0 {
                        flush_events(&batch, batch_len, &mut payload, &mut out);
                    }
                    (payload, out)
                })
                .unzip();
            log(&t0, "events parsed");
            let payloads_ref = &payloads;
            let (survivors, counts): (Vec<Vec<EvRec>>, Vec<[u32; NB2]>) = (0..NB1)
                .into_par_iter()
                .map(|b| {
                    let n: usize = keys_per_chunk.iter().map(|k| k[b].len()).sum();
                    // Linear-time dedup filter. Pass 1: two bitmaps indexed by the low bits of h (the top 8 bits
                    // chose this bucket): `seen` = some record hit this bit, `dup` = at least two records did.
                    // Pass 2: a record whose `dup` bit is clear shares h with no other record, hence its event_id
                    // is unique and it is emitted directly. Only `dup` records (true duplicates + bitmap/hash
                    // collisions, a small fraction) go through the exact sort-based grouping below.
                    let mut mbits = 1usize << 12;
                    while mbits < n * 8 {
                        mbits <<= 1;
                    }
                    let mmask = (mbits - 1) as u64;
                    let mut seen = vec![0u64; mbits / 64];
                    let mut dupb = vec![0u64; mbits / 64];
                    for k in &keys_per_chunk {
                        for e in &k[b] {
                            let x = e.h & mmask;
                            let (w, bit) = ((x >> 6) as usize, 1u64 << (x & 63));
                            unsafe {
                                let sw = seen.get_unchecked_mut(w);
                                let sv = *sw;
                                *sw = sv | bit;
                                *dupb.get_unchecked_mut(w) |= sv & bit;
                            }
                        }
                    }
                    drop(seen);
                    let mut surv = Vec::with_capacity(n);
                    let mut cnt = [0u32; NB2];
                    let mut emit = |p: &EventPayload| {
                        let pb_idx = ((p.rec.key >> 40) & 255) as usize;
                        cnt[pb_idx] += 1;
                        surv.push(p.rec);
                    };
                    let mut keys: Vec<EvKey> = Vec::with_capacity(n / 4 + 16);
                    for k in &keys_per_chunk {
                        for e in &k[b] {
                            let x = e.h & mmask;
                            if (unsafe { *dupb.get_unchecked((x >> 6) as usize) } >> (x & 63)) & 1 == 0 {
                                let p = &payloads_ref[(e.loc >> 32) as usize][(e.loc as u32) as usize];
                                if p.rec.is_valid() {
                                    emit(p);
                                }
                            } else {
                                keys.push(*e);
                            }
                        }
                    }
                    drop(dupb);
                    keys.sort_unstable_by_key(|k| (k.h, k.ingest, k.loc));
                    let mut s = 0;
                    while s < keys.len() {
                        let mut e = s + 1;
                        while e < keys.len() && keys[e].h == keys[s].h {
                            e += 1;
                        }
                        let g = &mut keys[s..e];
                        if g.len() == 1 {
                            let ci = (g[0].loc >> 32) as usize;
                            let row = (g[0].loc as u32) as usize;
                            let p = &payloads_ref[ci][row];
                            if p.rec.is_valid() {
                                emit(p);
                            }
                        } else {
                            let ci0 = (g[0].loc >> 32) as usize;
                            let row0 = (g[0].loc as u32) as usize;
                            let id0 = payloads_ref[ci0][row0].id;
                            let all_same = g[1..].iter().all(|k| {
                                let ci = (k.loc >> 32) as usize;
                                let row = (k.loc as u32) as usize;
                                eqs(payloads_ref[ci][row].id, id0)
                            });
                            if all_same {
                                let p0 = &payloads_ref[ci0][row0];
                                if p0.rec.is_valid() {
                                    emit(p0);
                                }
                            } else {
                                g.sort_by(|x, y| {
                                    let px = &payloads_ref[(x.loc >> 32) as usize][(x.loc as u32) as usize];
                                    let py = &payloads_ref[(y.loc >> 32) as usize][(y.loc as u32) as usize];
                                    px.id.cmp(py.id).then(x.ingest.cmp(&y.ingest)).then(x.loc.cmp(&y.loc))
                                });
                                let p0 = &payloads_ref[(g[0].loc >> 32) as usize][(g[0].loc as u32) as usize];
                                if p0.rec.is_valid() {
                                    emit(p0);
                                }
                                for w in 1..g.len() {
                                    let prev = &payloads_ref[(g[w - 1].loc >> 32) as usize][(g[w - 1].loc as u32) as usize];
                                    let curr = &payloads_ref[(g[w].loc >> 32) as usize][(g[w].loc as u32) as usize];
                                    if !eqs(curr.id, prev.id) {
                                        if curr.rec.is_valid() {
                                            emit(curr);
                                        }
                                    }
                                }
                            }
                        }
                        s = e;
                    }
                    (surv, cnt)
                })
                .unzip();
            drop(keys_per_chunk);
            drop(payloads);

            let mut offsets: Vec<[usize; NB2]> = vec![[0; NB2]; NB1];
            let mut totals = [0usize; NB2];
            for p in 0..NB2 {
                let mut sum = 0usize;
                for b in 0..NB1 {
                    offsets[b][p] = sum;
                    sum += counts[b][p] as usize;
                }
                totals[p] = sum;
            }

            let mut pb: Vec<Vec<EvRec>> = totals
                .iter()
                .map(|&tot| {
                    let mut v = Vec::with_capacity(tot);
                    unsafe { v.set_len(tot); }
                    v
                })
                .collect();

            struct RawPtr(*mut EvRec);
            unsafe impl Send for RawPtr {}
            unsafe impl Sync for RawPtr {}

            {
                let ptrs: Vec<RawPtr> = pb.iter_mut().map(|v| RawPtr(v.as_mut_ptr())).collect();
                (0..NB1).into_par_iter().for_each(|b| {
                    let mut cur_offsets = offsets[b];
                    for rec in &survivors[b] {
                        let pid = (rec.key >> 40) as usize;
                        let p = pid & 255;
                        let pos = cur_offsets[p];
                        cur_offsets[p] += 1;
                        unsafe {
                            *ptrs[p].0.add(pos) = *rec;
                        }
                    }
                });
            }
            drop(survivors);

            pb.par_iter_mut().for_each(|v| {
                v.sort_unstable_by_key(|e| e.key);
            });
            log(&t0, &format!("events valid: {}", pb.iter().map(|v| v.len()).sum::<usize>()));
            pb
        },
        || {
            const CLICK_BATCH: usize = 32;

            #[derive(Clone, Copy)]
            struct ClickBatchItem {
                id: S,
                user: S,
                user_h: u64,
                lo: u64,
                hi: u64,
                tag: u32,
                camp: u32,
                ts: i64,
                valid: bool,
            }

            const DUMMY_CLICK: ClickBatchItem = ClickBatchItem {
                id: &[],
                user: &[],
                user_h: 0,
                lo: 0,
                hi: 0,
                tag: 0,
                camp: 0,
                ts: 0,
                valid: false,
            };

            let (survivors, counts): (Vec<Vec<CkRec>>, Vec<[u32; NB2]>) = cchunks
                .par_iter()
                .map(|&(d, first)| {
                    let mut surv: Vec<CkRec> = Vec::with_capacity(d.len() / 50 + 16);
                    let mut cnt = [0u32; NB2];
                    let mut f = [None; 4];
                    let mut batch = [DUMMY_CLICK; CLICK_BATCH];
                    let mut batch_len = 0;

                    let mut flush_clicks = |batch: &[ClickBatchItem; CLICK_BATCH], len: usize, cnt: &mut [u32; NB2], surv: &mut Vec<CkRec>| {
                        if len == 0 { return; }
                        for item in &batch[..len] {
                            if item.valid && item.tag != 0 {
                                let slot_ptab = (item.user_h as usize) & ptab.mask;
                                let slot_intern = (item.user_h as usize) & interner_ref.mask;
                                unsafe {
                                    use core::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
                                    let p1 = ptab.slots.as_ptr().add(slot_ptab) as *const i8;
                                    _mm_prefetch(p1, _MM_HINT_T0);
                                    let p2 = interner_ref.slots.as_ptr().add(slot_intern) as *const i8;
                                    _mm_prefetch(p2, _MM_HINT_T0);
                                }
                            }
                        }

                        for item in &batch[..len] {
                            if item.valid {
                                let pid = resolve_precomputed(ptab, interner_ref, item.user, item.user_h, item.lo, item.hi, item.tag);
                                let ts_rel = (item.ts - base_ms) as u64;
                                let key = ((pid as u64) << 40) | ts_rel;
                                let p = (pid & 255) as usize;
                                cnt[p] += 1;
                                surv.push(CkRec { key, camp: item.camp, id: item.id });
                            }
                        }
                    };

                    for_lines_split::<4>(d, first, &mut f, |_l, f, _q| {
                        if let Some(id) = f[0] {
                            if id.is_empty() { return; }
                            let mut valid = false;
                            let mut user: S = &[];
                            let mut user_h = 0u64;
                            let mut lo = 0u64;
                            let mut hi = 0u64;
                            let mut tag = 0u32;
                            let mut camp = 0u32;
                            let mut ts = 0i64;

                            if let (Some(u), Some(c), Some(t_str)) = (f[1], f[2], f[3]) {
                                if !u.is_empty() {
                                    if let Some(t) = parse_ts(t_str) {
                                        if t >= ws - WEEK && t < we {
                                            if let Some(&cmp) = camp_id.get(c) {
                                                user = u;
                                                let (l, h_val, tg) = ukey(u);
                                                lo = l;
                                                hi = h_val;
                                                tag = tg;
                                                user_h = uhash(lo, hi, tg);
                                                camp = cmp;
                                                ts = t;
                                                valid = true;
                                            }
                                        }
                                    }
                                }
                            }

                            batch[batch_len] = ClickBatchItem {
                                id,
                                user,
                                user_h,
                                lo,
                                hi,
                                tag,
                                camp,
                                ts,
                                valid,
                            };
                            batch_len += 1;
                            if batch_len == CLICK_BATCH {
                                flush_clicks(&batch, batch_len, &mut cnt, &mut surv);
                                batch_len = 0;
                            }
                        }
                    });
                    if batch_len > 0 {
                        flush_clicks(&batch, batch_len, &mut cnt, &mut surv);
                    }
                    (surv, cnt)
                })
                .unzip();

            let mut offsets: Vec<[usize; NB2]> = vec![[0; NB2]; cchunks.len()];
            let mut totals = [0usize; NB2];
            for p in 0..NB2 {
                let mut sum = 0usize;
                for b in 0..cchunks.len() {
                    offsets[b][p] = sum;
                    sum += counts[b][p] as usize;
                }
                totals[p] = sum;
            }

            let mut cb: Vec<Vec<CkRec>> = totals
                .iter()
                .map(|&tot| {
                    let mut v = Vec::with_capacity(tot);
                    unsafe { v.set_len(tot); }
                    v
                })
                .collect();

            struct RawPtr(*mut CkRec);
            unsafe impl Send for RawPtr {}
            unsafe impl Sync for RawPtr {}

            {
                let ptrs: Vec<RawPtr> = cb.iter_mut().map(|v| RawPtr(v.as_mut_ptr())).collect();
                (0..cchunks.len()).into_par_iter().for_each(|b| {
                    let mut cur_offsets = offsets[b];
                    for rec in &survivors[b] {
                        let pid = (rec.key >> 40) as usize;
                        let p = pid & 255;
                        let pos = cur_offsets[p];
                        cur_offsets[p] += 1;
                        unsafe {
                            *ptrs[p].0.add(pos) = *rec;
                        }
                    }
                });
            }
            drop(survivors);

            cb.par_iter_mut().for_each(|v| {
                v.sort_unstable_by(|a, b| a.key.cmp(&b.key).then_with(|| a.id.cmp(b.id)));
                v.dedup_by(|a, b| a.key == b.key && eqs(a.id, b.id));
            });
            log(&t0, &format!("clicks valid: {}", cb.iter().map(|v| v.len()).sum::<usize>()));
            cb
        },
    );

    let full_names = build_names(&interner, &root_names);
    log(&t0, &format!("total persons (pids): {}", full_names.len()));

    // ---------------- per person ----------------
    let ev_pb_ref = &ev_pb;
    let cl_pb_ref = &cl_pb;
    let full_names_ref = &full_names;
    let cat_rank_ref = &cat_rank;
    let organic_idx = ncamp;
    let cd_stride = ndays;
    let cd_total_size = (ncamp + 1) * ndays;

    struct Acc {
        ud: Vec<(S, i32, i64, i64, i64, u32)>,
        cd: Vec<[i64; 4]>,
        cnt: Vec<u32>,
        active: Vec<u32>,
        pos: Vec<u32>,
        cats: Vec<(u32, i64)>,
    }
    let new_acc = || Acc {
        ud: Vec::new(),
        cd: vec![[0; 4]; cd_total_size],
        cnt: vec![0; ncamp],
        active: Vec::new(),
        pos: vec![u32::MAX; ncamp],
        cats: Vec::new(),
    };

    let result = (0..NB2)
        .into_par_iter()
        .fold(new_acc, |mut acc, b| {
            let ev_bucket = &ev_pb_ref[b];
            let cl_bucket = &cl_pb_ref[b];
            let mut c_idx = 0usize;
            let mut ev_start = 0usize;

            while ev_start < ev_bucket.len() {
                let cur_pid = (ev_bucket[ev_start].key >> 40) as u32;
                let p_person = full_names_ref[cur_pid as usize];
                let mut ev_end = ev_start + 1;
                while ev_end < ev_bucket.len() && ((ev_bucket[ev_end].key >> 40) as u32) == cur_pid {
                    ev_end += 1;
                }
                let ev = &ev_bucket[ev_start..ev_end];
                ev_start = ev_end;

                // Co-scan clicks
                while c_idx < cl_bucket.len() && ((cl_bucket[c_idx].key >> 40) as u32) < cur_pid {
                    c_idx += 1;
                }
                let cl_start = c_idx;
                while c_idx < cl_bucket.len() && ((cl_bucket[c_idx].key >> 40) as u32) == cur_pid {
                    c_idx += 1;
                }
                let cl = &cl_bucket[cl_start..c_idx];

                let (mut j, mut i, mut run) = (0usize, 0usize, 0usize);
                // day accumulator
                let mut cur_day = i32::MIN;
                let (mut d_s, mut d_e, mut d_r) = (0i64, 0i64, 0i64);
                acc.cats.clear();

                let mut k = 0;
                while k < ev.len() {
                    let start_rel = (ev[k].key & 0xFF_FFFF_FFFF) as i64;
                    let start = base_ms + start_rel;
                    let mut last_rel = start_rel;
                    let (mut ne, mut rev, mut hasp) = (0i64, 0i64, false);
                    let sess_begin = k;
                    while k < ev.len() {
                        let cur_rel = (ev[k].key & 0xFF_FFFF_FFFF) as i64;
                        if k != sess_begin && cur_rel - last_rel > GAP {
                            break;
                        }
                        last_rel = cur_rel;
                        ne += 1;
                        let cat = (ev[k].cat_price >> 32) as u32;
                        if cat != u32::MAX {
                            rev += (ev[k].cat_price as u32) as i64;
                            hasp = true;
                        }
                        k += 1;
                    }
                    let day = start.div_euclid(DAY) as i32;
                    if day != cur_day {
                        if cur_day != i32::MIN {
                            let mut best: Option<(i64, u32, u32)> = None;
                            for &(c, a) in &acc.cats {
                                let r = cat_rank_ref[c as usize];
                                if best.map_or(true, |(ba, br, _)| a > ba || (a == ba && r < br)) {
                                    best = Some((a, r, c));
                                }
                            }
                            acc.ud.push((p_person, cur_day, d_s, d_e, d_r, best.map_or(u32::MAX, |b| b.2)));
                            acc.cats.clear();
                        }
                        cur_day = day;
                        d_s = 0;
                        d_e = 0;
                        d_r = 0;
                    }
                    d_s += 1;
                    d_e += ne;
                    d_r += rev;
                    if hasp {
                        for x in &ev[sess_begin..k] {
                            let cat = (x.cat_price >> 32) as u32;
                            if cat != u32::MAX {
                                let price = (x.cat_price as u32) as i64;
                                if let Some(p) = acc.cats.iter_mut().find(|p| p.0 == cat) {
                                    p.1 += price;
                                } else {
                                    acc.cats.push((cat, price));
                                }
                            }
                        }
                    }
                    let day_offset = (day - ws_day) as usize;
                    if cl.is_empty() {
                        let cell_idx = organic_idx * cd_stride + day_offset;
                        acc.cd[cell_idx][0] += 1;
                        acc.cd[cell_idx][1] += hasp as i64;
                        acc.cd[cell_idx][2] += rev;
                    } else {
                        // clicks: advance window [start - WEEK, start]
                        while j < cl.len() {
                            let cl_rel = (cl[j].key & 0xFF_FFFF_FFFF) as i64;
                            if cl_rel > start_rel {
                                break;
                            }
                            if j == 0 || (cl[j].key & 0xFF_FFFF_FFFF) != (cl[j - 1].key & 0xFF_FFFF_FFFF) {
                                run = j;
                            }
                            let c = cl[j].camp as usize;
                            if acc.cnt[c] == 0 {
                                acc.pos[c] = acc.active.len() as u32;
                                acc.active.push(c as u32);
                            }
                            acc.cnt[c] += 1;
                            j += 1;
                        }
                        while i < j {
                            let cl_rel = (cl[i].key & 0xFF_FFFF_FFFF) as i64;
                            if cl_rel >= start_rel - WEEK {
                                break;
                            }
                            let c = cl[i].camp as usize;
                            acc.cnt[c] -= 1;
                            if acc.cnt[c] == 0 {
                                let p = acc.pos[c] as usize;
                                let lastc = *acc.active.last().unwrap();
                                acc.active.swap_remove(p);
                                if lastc as usize != c {
                                    acc.pos[lastc as usize] = p as u32;
                                }
                                acc.pos[c] = u32::MAX;
                            }
                            i += 1;
                        }
                        let credited = if j > 0 && start_rel - ((cl[run].key & 0xFF_FFFF_FFFF) as i64) <= WEEK { cl[run].camp as usize } else { organic_idx };
                        let cell_idx = credited * cd_stride + day_offset;
                        acc.cd[cell_idx][0] += 1;
                        acc.cd[cell_idx][1] += hasp as i64;
                        acc.cd[cell_idx][2] += rev;
                        for &c in &acc.active {
                            acc.cd[(c as usize) * cd_stride + day_offset][3] += 1;
                        }
                    }
                }
                if cur_day != i32::MIN {
                    let mut best: Option<(i64, u32, u32)> = None;
                    for &(c, a) in &acc.cats {
                        let r = cat_rank_ref[c as usize];
                        if best.map_or(true, |(ba, br, _)| a > ba || (a == ba && r < br)) {
                            best = Some((a, r, c));
                        }
                    }
                    acc.ud.push((p_person, cur_day, d_s, d_e, d_r, best.map_or(u32::MAX, |b| b.2)));
                    acc.cats.clear();
                }
                for &c in &acc.active {
                    acc.cnt[c as usize] = 0;
                    acc.pos[c as usize] = u32::MAX;
                }
                acc.active.clear();
            }
            acc
        })
        .reduce(
            new_acc,
            |mut a, b| {
                a.ud.extend(b.ud);
                for (x, y) in a.cd.iter_mut().zip(&b.cd) {
                    x[0] += y[0];
                    x[1] += y[1];
                    x[2] += y[2];
                    x[3] += y[3];
                }
                a
            },
        );

    let (ud, cd) = (result.ud, result.cd);
    let cd_rows = cd.iter().filter(|r| r[0] > 0 || r[3] > 0).count();
    log(&t0, &format!("user_daily rows {}, campaign_daily rows {}", ud.len(), cd_rows));

    // ---------------- write parquet ----------------
    write_outputs(&out, &ud, &cd, ws_day, ndays, ncamp, &cat_names, &camp_names);
    log(&t0, "done");
    use std::io::Write;
    let _ = std::io::stdout().flush();
    let _ = std::io::stderr().flush();
    std::process::exit(0);
}

fn write_outputs(
    out: &Path,
    ud: &[(S, i32, i64, i64, i64, u32)],
    cd: &[[i64; 4]],
    ws_day: i32,
    ndays: usize,
    ncamp: usize,
    cat_names: &[S],
    camp_names: &[(S, S)],
) {
    use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    let props = || WriterProperties::builder().set_compression(Compression::UNCOMPRESSED).build();

    let udir = out.join("user_daily");
    let cdir = out.join("campaign_daily");
    fs::create_dir_all(&udir).unwrap();
    fs::create_dir_all(&cdir).unwrap();

    let write_ud = || {
        let us = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("day", DataType::Date32, false),
            Field::new("sessions", DataType::Int64, false),
            Field::new("events", DataType::Int64, false),
            Field::new("revenue_cents", DataType::Int64, false),
            Field::new("top_category", DataType::Utf8, true),
        ]));
        let parts = 8usize;
        let per = ud.len().div_ceil(parts).max(1);
        let pieces: Vec<&[(S, i32, i64, i64, i64, u32)]> = if ud.is_empty() { vec![ud] } else { ud.chunks(per).collect() };
        pieces.par_iter().enumerate().for_each(|(pi, rows)| {
            let cols: Vec<ArrayRef> = vec![
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| utf8(r.0)))),
                Arc::new(Date32Array::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.3))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.4))),
                Arc::new(StringArray::from_iter(
                    rows.iter().map(|r| if r.5 == u32::MAX { None } else { Some(utf8(cat_names[r.5 as usize])) }),
                )),
            ];
            let b = RecordBatch::try_new(us.clone(), cols).unwrap();
            let f = File::create(udir.join(format!("part-{pi}.parquet"))).unwrap();
            let mut w = ArrowWriter::try_new(f, us.clone(), Some(props())).unwrap();
            w.write(&b).unwrap();
            w.close().unwrap();
        });
    };

    let write_cd = || {
        let cs = Arc::new(Schema::new(vec![
            Field::new("campaign_id", DataType::Utf8, false),
            Field::new("channel", DataType::Utf8, false),
            Field::new("day", DataType::Date32, false),
            Field::new("sessions", DataType::Int64, false),
            Field::new("purchasing_sessions", DataType::Int64, false),
            Field::new("revenue_cents", DataType::Int64, false),
            Field::new("assisted_sessions", DataType::Int64, false),
        ]));

        let mut camp_col: Vec<std::borrow::Cow<str>> = Vec::new();
        let mut chan_col: Vec<std::borrow::Cow<str>> = Vec::new();
        let mut day_col: Vec<i32> = Vec::new();
        let mut s0: Vec<i64> = Vec::new();
        let mut s1: Vec<i64> = Vec::new();
        let mut s2: Vec<i64> = Vec::new();
        let mut s3: Vec<i64> = Vec::new();

        for c in 0..=ncamp {
            for d in 0..ndays {
                let r = cd[c * ndays + d];
                if r[0] > 0 || r[3] > 0 {
                    let (cid, ch) = if c == ncamp {
                        (std::borrow::Cow::Borrowed("organic"), std::borrow::Cow::Borrowed("organic"))
                    } else {
                        let (a, b) = camp_names[c];
                        (utf8(a), utf8(b))
                    };
                    camp_col.push(cid);
                    chan_col.push(ch);
                    day_col.push(ws_day + d as i32);
                    s0.push(r[0]);
                    s1.push(r[1]);
                    s2.push(r[2]);
                    s3.push(r[3]);
                }
            }
        }

        let cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from_iter_values(camp_col)),
            Arc::new(StringArray::from_iter_values(chan_col)),
            Arc::new(Date32Array::from_iter_values(day_col)),
            Arc::new(Int64Array::from_iter_values(s0)),
            Arc::new(Int64Array::from_iter_values(s1)),
            Arc::new(Int64Array::from_iter_values(s2)),
            Arc::new(Int64Array::from_iter_values(s3)),
        ];
        let b = RecordBatch::try_new(cs.clone(), cols).unwrap();
        let f = File::create(cdir.join("part-0.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, cs.clone(), Some(props())).unwrap();
        w.write(&b).unwrap();
        w.close().unwrap();
    };

    rayon::join(write_ud, write_cd);
}
