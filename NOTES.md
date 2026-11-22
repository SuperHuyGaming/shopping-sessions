# own5b notes (Change 1 SIMD quotes, Change 5 date cache / fast paths, Rank 4 own Parquet writer + fast exit)

Base: 2e8f942 (v12c). Edit: AVX-512 structural scan handles quoted lines in-SIMD (quote-only-at-field-edges check,
scalar split only for genuinely odd lines); per-thread YYYY-MM-DD date cache; own uncompressed PLAIN Parquet writer
(arrow/parquet crates dropped); _exit after flush.

## Gates (2026-11-22, scorer flags, --memory 12g, cpuset 0-7)
- fixtures 36/36 (score.py --quick); shop5-dev2 vs dev-answer: MATCH.
- shop5b-20m / shop5b-60m: output IDENTICAL to baseline shop-own5base (compare.py MATCH both).
- vs the ck refs (exp/out5-shop5b-*-shop-ck): DIFFER by ONE user_daily row, and the BASELINE image differs the same way.
  Row: uaw6fbgdh 2027-12-22: ours events=17, ck ref 16. Independent Python check of that person's 137 raw event lines
  (rules.md steps 1-6, linked id ubt2cgsiv) gives 17 -> the ck ref looks wrong for this row (candidate: the F2
  `2027-12-22T05:06:42-09:00` event e3e4tbkzy). Refs should be re-verified before using them as a gate.

## Timing (interleaved, wall ms incl. docker start)
- 20M x5: base 2541 2024 2021 1990 2019 (med 2021); own5b 1927 1980 1989 1846 1831 (med 1927).
  SS_LOG x3 internal 'done': base 1.82/1.59/1.54 s, own5b 1.44/1.42/1.43 s (events parse 1.08-1.32 -> 0.95-0.98 s).
- 60M x3: base 8126 6271 5976 (med 6271); own5b 7028 5959 6135 (med 6135). Second set: base 8973 6410 6028,
  own5b 5969 6540 5731. 60M parse is ~flat (3.7 s both); gain there is noise-level, 60M looks memory/IO bound.
Verdict: exact and faster on 20M (~5-8% wall, ~0.15 s internal), neutral-to-slightly-faster on 60M. KEEP.
