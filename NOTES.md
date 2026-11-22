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

# own5a notes (clean-room; scope: plan Change 2 / methods Rank 1 + Change 4 / Rank 2)

## Changes (on top of v12c 2e8f942)
- d309603: tiered exact user keys (short ids and 32-hex-suffix ids packed inline in 16 bytes + tag,
  long ids in a fallback map owned by the table, no global fallback mutex); bitmap-filtered linear-time
  event dedup (records whose h-bit is set only once are unique and emitted directly; only collision
  candidates go through the exact sort/compare path).
- uncommitted -> committed here: links use ONE open-addressed table (`PTab::get_or_insert`) as the
  id -> union-find-node index during merging, then rewritten in place to id -> pid (parent is fully
  flattened by the root-numbering pass). Removes the separate `Map<S,u32>` and the second insert pass.

## Gates (12g, scorer flags)
- shop5-dev2: MATCH. shop5b-60m: MATCH vs out5-shop5b-60m-shop-ck.
- shop5b-20m: DIFFER vs out5-shop5b-20m-shop-ck on ONE row (uaw6fbgdh 2027-12-22: events 17 vs ref 16);
  MATCH vs out5-shop5b-20m-shop-ss5-aqilc. The v12c baseline image (shop-own5) gives the same 17 now.
  Hand recomputation from the raw lines (scratch/own5a_an.py over every copy of this person's events)
  gives 17, so the ck ref looks wrong on that row. BUT: earlier runs of shop-own5a1/a2 at 01:30 matched ck
  (16) with identical image+data, so some engine is nondeterministic. Could not reproduce 16 in 8+ runs
  (12g, 24g, --cpus 2). Open issue, affects baseline too, not introduced here.
- score.py --quick: fixtures 36/36, dev MATCH.

## Timing (interleaved, ms, medians)
| image | 20M (5 runs) | 60M (3 runs) |
| base v12c | 1922 | 7082 |
| a2 (d309603) | 1720 | 4863 |
| a3 (this commit) | 1688 | 4710 |
60M runs are noisy (first run of each image slower). Keep a3.

# own5 merged (own5a 674b871 + own5b 5dc16eb)

Clean-room integration combining:
- own5a: three-tier compact exact user keys (ukey / uhash), single links table (`PTab::get_or_insert`), bitmap-filtered linear event dedup (`dupb` / `seen`).
- own5b: SIMD quoted-line split (`simd_for_lines`), per-thread date-prefix cache (`DateCache`), minimal uncompressed Parquet writer (`pq`), fast `_exit(0)`.

