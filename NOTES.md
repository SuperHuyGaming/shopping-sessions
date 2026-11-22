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
