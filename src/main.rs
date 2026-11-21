//! Shopping Sessions engine (Rust). Implements rules.md exactly; v1 DuckDB pipeline (git history) is the reference.
use memchr::{memchr, memchr_iter};
use memmap2::Mmap;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::{self, File};
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
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

fn parse_ts(s: S) -> Option<i64> {
    if s.iter().all(|c| c.is_ascii_digit()) {
        let mut i = 0;
        while i < s.len() && s[i] == b'0' {
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
        return if v > i64::MAX as u64 { None } else { Some(v as i64) };
    }
    let n = s.len();
    if n != 20 && n != 25 {
        return None;
    }
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
}

fn parse_price(s: Option<S>) -> i64 {
    let Some(s) = s else { return -1 };
    if !s.iter().all(|c| c.is_ascii_digit()) {
        return -1;
    }
    let mut i = 0;
    while i < s.len() && s[i] == b'0' {
        i += 1;
    }
    let t = &s[i..];
    if t.is_empty() || t.len() > 8 {
        return -1;
    }
    let v: i64 = t.iter().fold(0, |a, &c| a * 10 + (c - b'0') as i64);
    if (1..=10_000_000).contains(&v) { v } else { -1 }
}

// ---------------- records ----------------
struct Ev {
    h: u64,
    id: S,
    user: Option<S>,
    ts: Option<S>,
    ingest: i64,
    et: Option<S>,
    prod: Option<S>,
    price: Option<S>,
}

#[derive(Clone, Copy)]
struct VE {
    ph: u64,
    person: S,
    ts: i64,
    cat: u32, // purchase category id, u32::MAX for non-purchase
    price: i64,
}

/// v4 parsed event: dedup key + the validated record (ve.cat == INVALID when the row fails validation).
struct Rec {
    h: u64,
    ingest: i64,
    ord: u64,
    id: S,
    ve: VE,
}
const INVALID: u32 = u32::MAX - 1;
const BAD: VE = VE { ph: 0, person: &[], ts: 0, cat: INVALID, price: 0 };
const NB1: usize = 512;
const SH1: u32 = 64 - 9;
const NB2: usize = 256;
const SH2: u32 = 64 - 8;

/// v is sorted by ph first. Inside each equal-ph run that mixes different people (64-bit hash collision),
/// re-sort that run with the exact comparator so each person's rows are contiguous and ordered.
fn fix_ph<T>(v: &mut [T], ph: impl Fn(&T) -> u64, same: impl Fn(&T, &T) -> bool, cmp: impl Fn(&T, &T) -> std::cmp::Ordering) {
    let mut s = 0;
    while s < v.len() {
        let mut e = s + 1;
        let mut mixed = false;
        while e < v.len() && ph(&v[e]) == ph(&v[s]) {
            if !same(&v[e], &v[s]) {
                mixed = true;
            }
            e += 1;
        }
        if mixed {
            v[s..e].sort_by(&cmp);
        }
        s = e;
    }
}

#[derive(Clone, Copy, Default)]
struct Slot {
    lo: u64,
    hi: u64,
    ph: u64,
    person: S,
    len: u32,
}
/// user id -> (hash of person, person). Ids up to 16 bytes are compared inline (no pointer chase, no memcmp).
/// (lo, hi, len) identifies the id exactly: bytes [0..8] and [len-8..len] for len >= 8, else packed bytes.
struct PTab {
    mask: usize,
    slots: Vec<Slot>,
    long: Map<S, S>,
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
impl PTab {
    fn insert(&mut self, id: S, person: S) {
        if id.len() > 16 || id.is_empty() {
            self.long.insert(id, person);
            return;
        }
        let (lo, hi) = key16(id);
        let mut i = (xxh3_64(id) as usize) & self.mask;
        loop {
            let sl = &mut self.slots[i];
            if sl.len == 0 {
                *sl = Slot { lo, hi, ph: xxh3_64(person), person, len: id.len() as u32 };
                return;
            }
            if sl.len as usize == id.len() && sl.lo == lo && sl.hi == hi {
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
    /// (xxh3(person), person); unlinked ids are their own person.
    #[inline(always)]
    fn get(&self, u: S) -> (u64, S) {
        let h = xxh3_64(u);
        if u.len() > 16 || u.is_empty() {
            let p = *self.long.get(u).unwrap_or(&u);
            return (xxh3_64(p), p);
        }
        let (lo, hi) = key16(u);
        let mut i = (h as usize) & self.mask;
        loop {
            let sl = &self.slots[i];
            if sl.len == 0 {
                return (h, u);
            }
            if sl.len as usize == u.len() && sl.lo == lo && sl.hi == hi {
                return (sl.ph, sl.person);
            }
            i = (i + 1) & self.mask;
        }
    }
}

struct Ck {
    h: u64,
    id: S,
    user: Option<S>,
    camp: Option<S>,
    ts: Option<S>,
}

/// keys sorted by (hash, ingest, row). Returns the kept row of each distinct id: the first in (ingest, row) order.
/// `same` compares ids; on a 64-bit hash collision (different ids, same hash) the group is re-sorted with `cmp`.
fn dedup_keys(
    keys: &[(u64, i64, u32)],
    same: impl Fn(usize, usize) -> bool + Sync,
    cmp: impl Fn(usize, usize) -> std::cmp::Ordering + Sync,
) -> Vec<u32> {
    let n = keys.len();
    let starts: Vec<usize> = (0..n).into_par_iter().filter(|&i| i == 0 || keys[i].0 != keys[i - 1].0).collect();
    starts
        .par_iter()
        .enumerate()
        .flat_map_iter(|(k, &s)| {
            let e = if k + 1 < starts.len() { starts[k + 1] } else { n };
            let g = &keys[s..e];
            let first = g[0].2 as usize;
            if g.iter().all(|x| same(x.2 as usize, first)) {
                vec![g[0].2]
            } else {
                let mut rows: Vec<u32> = g.iter().map(|x| x.2).collect();
                rows.sort_by(|&a, &b| cmp(a as usize, b as usize));
                let mut out = vec![rows[0]];
                for w in rows.windows(2) {
                    if !same(w[0] as usize, w[1] as usize) {
                        out.push(w[1]);
                    }
                }
                out
            }
        })
        .collect()
}

#[derive(Clone, Copy)]
struct VC {
    ph: u64,
    person: S,
    ts: i64,
    id: S,
    camp: u32,
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
    // flat open-addressing table: id bytes inline (<=16 B), person + its hash stored in the slot
    let mut cap = 1usize;
    while cap < names.len() * 2 + 16 {
        cap <<= 1;
    }
    let mut ptab = PTab { mask: cap - 1, slots: vec![Slot::default(); cap], long: Map::default() };
    for i in 0..names.len() as u32 {
        let r = find(&mut parent, i);
        ptab.insert(names[i as usize], names[r as usize]);
    }
    log(&t0, &format!("links: {} pairs, {} ids", pairs.len(), names.len()));
    let ptab = &ptab;


    // ---------------- events || clicks (independent until the per-person pass) ----------------
    let (ves, vcs) = rayon::join(
        || {
            let echunks = chunks(&data.join("events"));
            // v4 Phase A: parse + validate inline (sequential memory), scatter by event_id hash into NB1 buckets.
            let validate = |user: Option<S>, ts: Option<S>, et: Option<S>, prod: Option<S>, price: Option<S>, ingest: i64| -> Option<VE> {
                let user = user?;
                let ts = parse_ts(ts?)?;
                if ts < ws || ts >= we || prod.is_none() {
                    return None;
                }
                let et = match et {
                    Some(e) if eqs(e, b"view") => 1,
                    Some(e) if eqs(e, b"cart") => 2,
                    Some(e) if eqs(e, b"purchase") => 3,
                    _ => return None,
                };
                let price = if et == 3 { parse_price(price) } else { 0 };
                if et == 3 && price < 1 {
                    return None;
                }
                if ingest - ts > DAY {
                    return None;
                }
                let (ph, person) = ptab.get(user);
                let cat = if et == 3 { *prod_cat.get(prod.unwrap()).unwrap_or(&unknown) } else { u32::MAX };
                Some(VE { ph, person, ts, cat, price })
            };
            let parts: Vec<Vec<Vec<Rec>>> = echunks
                .par_iter()
                .enumerate()
                .map(|(ci, &(d, first))| {
                    let cap = d.len() / 70 / NB1 + 16;
                    let mut out: Vec<Vec<Rec>> = (0..NB1).map(|_| Vec::with_capacity(cap)).collect();
                    let mut f = [None; 7];
                    let mut ln: u64 = 0;
                    for_lines_split::<7>(d, first, &mut f, |_l, f, _q| {
                        {
                            if let Some(id) = f[0] {
                                let h = xxh3_64(id);
                                let ingest = f[3].and_then(parse_ts).unwrap_or(i64::MAX);
                                let ve = validate(f[1], f[2], f[4], f[5], f[6], ingest).unwrap_or(BAD);
                                out[(h >> SH1) as usize].push(Rec { h, ingest, ord: ((ci as u64) << 32) | ln, id, ve });
                                ln += 1;
                            }
                        }
                    });
                    out
                })
                .collect();
            log(&t0, "events parsed");
            // Phase B: per id-bucket dedup (first in (ingest, file order)); exact id check on hash collisions.
            // Survivors that are valid are scattered by person hash into NB2 buckets.
            let outs: Vec<Vec<Vec<VE>>> = (0..NB1)
                .into_par_iter()
                .map(|b| {
                    let n: usize = parts.iter().map(|p| p[b].len()).sum();
                    let mut keys: Vec<(u64, i64, u64, &Rec)> = Vec::with_capacity(n);
                    for p in &parts {
                        keys.extend(p[b].iter().map(|r| (r.h, r.ingest, r.ord, r)));
                    }
                    keys.sort_unstable_by_key(|k| (k.0, k.1, k.2));
                    let mut out: Vec<Vec<VE>> = (0..NB2).map(|_| Vec::with_capacity(n / NB2 + 16)).collect();
                    let mut emit = |r: &Rec| {
                        if r.ve.cat != INVALID {
                            out[(r.ve.ph >> SH2) as usize].push(r.ve);
                        }
                    };
                    let mut s = 0;
                    while s < keys.len() {
                        let mut e = s + 1;
                        while e < keys.len() && keys[e].0 == keys[s].0 {
                            e += 1;
                        }
                        let g = &mut keys[s..e];
                        if g.len() == 1 || g.iter().all(|k| eqs(k.3.id, g[0].3.id)) {
                            emit(g[0].3);
                        } else {
                            g.sort_by(|x, y| x.3.id.cmp(y.3.id).then(x.1.cmp(&y.1)).then(x.2.cmp(&y.2)));
                            emit(g[0].3);
                            for w in 1..g.len() {
                                if !eqs(g[w].3.id, g[w - 1].3.id) {
                                    emit(g[w].3);
                                }
                            }
                        }
                        s = e;
                    }
                    out
                })
                .collect();
            drop(parts);
            // Phase C: per person-bucket concat + local sort by (ph, person, ts). Bucket order == global (ph) order.
            let pb: Vec<Vec<VE>> = (0..NB2)
                .into_par_iter()
                .map(|p| {
                    let n: usize = outs.iter().map(|o| o[p].len()).sum();
                    let mut v: Vec<VE> = Vec::with_capacity(n);
                    for o in &outs {
                        v.extend_from_slice(&o[p]);
                    }
                    v.sort_unstable_by_key(|x| (x.ph, x.ts));
                    fix_ph(&mut v, |x| x.ph, |a, b| eqs(a.person, b.person), |a, b| a.person.cmp(b.person).then(a.ts.cmp(&b.ts)));
                    v
                })
                .collect();
            drop(outs);
            log(&t0, &format!("events valid: {}", pb.iter().map(|v| v.len()).sum::<usize>()));
            pb
        },
        || {
            let cchunks = chunks(&data.join("clicks"));
            // v6: BRIEF: "Copies of a click have identical values, so keep any one". So: parse + validate inline,
            // scatter straight into person buckets, sort locally, drop adjacent identical copies.
            let validate = |id: S, user: Option<S>, camp: Option<S>, ts: Option<S>| -> Option<VC> {
                let user = user?;
                let ts = parse_ts(ts?)?;
                if ts < ws - WEEK || ts >= we {
                    return None;
                }
                let camp = *camp_id.get(camp?)?;
                let (ph, person) = ptab.get(user);
                Some(VC { ph, person, ts, id, camp })
            };
            let parts: Vec<Vec<Vec<VC>>> = cchunks
                .par_iter()
                .map(|&(d, first)| {
                    let cap = d.len() / 50 / NB2 + 16;
                    let mut out: Vec<Vec<VC>> = (0..NB2).map(|_| Vec::with_capacity(cap)).collect();
                    let mut f = [None; 4];
                    for_lines_split::<4>(d, first, &mut f, |_l, f, _q| {
                        {
                            if let Some(id) = f[0] {
                                if let Some(vc) = validate(id, f[1], f[2], f[3]) {
                                    out[(vc.ph >> SH2) as usize].push(vc);
                                }
                            }
                        }
                    });
                    out
                })
                .collect();
            let cb: Vec<Vec<VC>> = (0..NB2)
                .into_par_iter()
                .map(|p| {
                    let n: usize = parts.iter().map(|o| o[p].len()).sum();
                    let mut v: Vec<VC> = Vec::with_capacity(n);
                    for o in &parts {
                        v.extend_from_slice(&o[p]);
                    }
                    v.sort_unstable_by(|a, b| (a.ph, a.ts).cmp(&(b.ph, b.ts)).then_with(|| a.id.cmp(b.id)));
                    fix_ph(&mut v, |x| x.ph, |a, b| eqs(a.person, b.person), |a, b| {
                        a.person.cmp(b.person).then(a.ts.cmp(&b.ts)).then_with(|| a.id.cmp(b.id))
                    });
                    v.dedup_by(|a, b| a.ts == b.ts && eqs(a.id, b.id) && eqs(a.person, b.person));
                    v
                })
                .collect();
            drop(parts);
            log(&t0, &format!("clicks valid: {}", cb.iter().map(|v| v.len()).sum::<usize>()));
            cb
        },
    );

    // ---------------- per person ----------------
    let groups: Vec<&[VE]> = ves
        .par_iter()
        .flat_map_iter(|v| {
            let mut g = Vec::new();
            let mut s0 = 0;
            for i in 1..=v.len() {
                if i == v.len() || v[i].ph != v[i - 1].ph || !eqs(v[i].person, v[i - 1].person) {
                    g.push(&v[s0..i]);
                    s0 = i;
                }
            }
            g
        })
        .collect();

    struct Acc {
        ud: Vec<(S, i32, i64, i64, i64, u32)>,
        cd: Map<(u32, i32), [i64; 4]>,
        cnt: Vec<u32>,
        active: Vec<u32>,
        pos: Vec<u32>,
        cats: Vec<(u32, i64)>,
    }
    let new_acc = || Acc {
        ud: Vec::new(),
        cd: Map::default(),
        cnt: vec![0; ncamp],
        active: Vec::new(),
        pos: vec![u32::MAX; ncamp],
        cats: Vec::new(),
    };
    let vcs_ref = &vcs;
    let cat_rank_ref = &cat_rank;
    let result = groups
        .par_iter()
        .fold(new_acc, |mut acc, ev: &&[VE]| {
            let ev: &[VE] = ev;
            let key = (ev[0].ph, ev[0].person);
            let cv = &vcs_ref[(key.0 >> SH2) as usize];
            let lo = cv.partition_point(|c| (c.ph, c.person) < key);
            let hi = lo + cv[lo..].partition_point(|c| (c.ph, c.person) == key);
            let cl = &cv[lo..hi];
            let (mut j, mut i, mut run) = (0usize, 0usize, 0usize);
            // day accumulator
            let mut cur_day = i32::MIN;
            let (mut d_s, mut d_e, mut d_r) = (0i64, 0i64, 0i64);
            acc.cats.clear();
            let flush = |acc: &mut Acc, day: i32, ds: i64, de: i64, dr: i64| {
                let mut best: Option<(i64, u32, u32)> = None;
                for &(c, a) in &acc.cats {
                    let r = cat_rank_ref[c as usize];
                    if best.map_or(true, |(ba, br, _)| a > ba || (a == ba && r < br)) {
                        best = Some((a, r, c));
                    }
                }
                acc.ud.push((ev[0].person, day, ds, de, dr, best.map_or(u32::MAX, |b| b.2)));
                acc.cats.clear();
            };
            let mut k = 0;
            while k < ev.len() {
                // one session
                let start = ev[k].ts;
                let mut last = start;
                let (mut ne, mut rev, mut hasp) = (0i64, 0i64, false);
                let sess_begin = k;
                while k < ev.len() && (k == sess_begin || ev[k].ts - last <= GAP) {
                    last = ev[k].ts;
                    ne += 1;
                    if ev[k].cat != u32::MAX {
                        rev += ev[k].price;
                        hasp = true;
                    }
                    k += 1;
                }
                let day = start.div_euclid(DAY) as i32;
                if day != cur_day {
                    if cur_day != i32::MIN {
                        flush(&mut acc, cur_day, d_s, d_e, d_r);
                    }
                    cur_day = day;
                    d_s = 0;
                    d_e = 0;
                    d_r = 0;
                }
                d_s += 1;
                d_e += ne;
                d_r += rev;
                for x in &ev[sess_begin..k] {
                    if x.cat != u32::MAX {
                        if let Some(p) = acc.cats.iter_mut().find(|p| p.0 == x.cat) {
                            p.1 += x.price;
                        } else {
                            acc.cats.push((x.cat, x.price));
                        }
                    }
                }
                // clicks: advance window [start - WEEK, start]
                while j < cl.len() && cl[j].ts <= start {
                    if j == 0 || cl[j].ts != cl[j - 1].ts {
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
                while i < j && cl[i].ts < start - WEEK {
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
                let credited = if j > 0 && start - cl[run].ts <= WEEK { cl[run].camp } else { ORGANIC };
                let r = acc.cd.entry((credited, day)).or_insert([0; 4]);
                r[0] += 1;
                r[1] += hasp as i64;
                r[2] += rev;
                for &c in &acc.active {
                    acc.cd.entry((c, day)).or_insert([0; 4])[3] += 1;
                }
            }
            if cur_day != i32::MIN {
                flush(&mut acc, cur_day, d_s, d_e, d_r);
            }
            // reset click window state
            for &c in &acc.active {
                acc.cnt[c as usize] = 0;
                acc.pos[c as usize] = u32::MAX;
            }
            acc.active.clear();
            acc
        })
        .map(|a| (a.ud, a.cd))
        .reduce(
            || (Vec::new(), Map::default()),
            |(mut ua, mut ca), (ub, cb)| {
                if ua.len() < ub.len() {
                    let mut ub = ub;
                    ub.extend(ua);
                    ua = ub;
                } else {
                    ua.extend(ub);
                }
                for (k, v) in cb {
                    let r = ca.entry(k).or_insert([0; 4]);
                    for t in 0..4 {
                        r[t] += v[t];
                    }
                }
                (ua, ca)
            },
        );
    let (ud, cd) = result;
    log(&t0, &format!("user_daily rows {}, campaign_daily rows {}", ud.len(), cd.len()));

    // ---------------- write parquet ----------------
    write_outputs(&out, &ud, &cd, &cat_names, &camp_names);
    log(&t0, "done");
    // 1BRC trick: skip destructors / allocator teardown of GBs of buffers; outputs are already written + closed.
    std::process::exit(0);
}

fn write_outputs(
    out: &Path,
    ud: &[(S, i32, i64, i64, i64, u32)],
    cd: &Map<(u32, i32), [i64; 4]>,
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

    let cs = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    let rows: Vec<(&(u32, i32), &[i64; 4])> = cd.iter().collect();
    let name = |c: u32| -> (std::borrow::Cow<'_, str>, std::borrow::Cow<'_, str>) {
        if c == ORGANIC {
            ("organic".into(), "organic".into())
        } else {
            let (a, b) = camp_names[c as usize];
            (utf8(a), utf8(b))
        }
    };
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| name(r.0 .0).0))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| name(r.0 .0).1))),
        Arc::new(Date32Array::from_iter_values(rows.iter().map(|r| r.0 .1))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1[0]))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1[1]))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1[2]))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1[3]))),
    ];
    let b = RecordBatch::try_new(cs.clone(), cols).unwrap();
    let f = File::create(cdir.join("part-0.parquet")).unwrap();
    let mut w = ArrowWriter::try_new(f, cs.clone(), Some(props())).unwrap();
    w.write(&b).unwrap();
    w.close().unwrap();
}
