#!/usr/bin/env python3
"""Shopping Sessions pipeline (DuckDB). Implements rules.md exactly.

Reads  $DATA_DIR, writes $OUTPUT_DIR/{user_daily,campaign_daily}/part-0.parquet, temp in $SCRATCH_DIR.
CSV lines are read raw (read_text) so `\r`, quotes and malformed lines follow rules.md, not DuckDB's CSV
reader. Linked IDs are merged with a union-find in Python.
"""
import json
import os
import sys

import duckdb

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))
DAY_MS = 86_400_000
WEEK_MS = 7 * DAY_MS
GAP_MS = 1_800_000

con = duckdb.connect()
con.execute(f"SET threads = {NCPU}")
con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.75))}GB'")
con.execute(f"SET temp_directory = '{os.path.join(SCRATCH, 'duckdb_tmp')}'")
con.execute("SET preserve_insertion_order = false")


import time as _time

_T0 = _time.perf_counter()


def q(sql, params=None):
    t = _time.perf_counter()
    r = con.execute(sql, params) if params is not None else con.execute(sql)
    dt = _time.perf_counter() - t
    if dt > 0.5:
        print(f"[{_time.perf_counter() - _T0:7.1f}s] {dt:6.1f}s  {' '.join(sql.split())[:70]}", file=sys.stderr, flush=True)
    return r


def log(msg):
    print(msg, file=sys.stderr, flush=True)


# ---------- helpers (SQL macros) ----------
# A field is valid if it has no quote, or is wrapped in quotes with none inside. Value: unwrapped, '' -> NULL.
q("""CREATE MACRO fld_ok(p) AS strpos(p, '"') = 0
      OR (length(p) >= 2 AND left(p, 1) = '"' AND right(p, 1) = '"' AND strpos(substr(p, 2, length(p) - 2), '"') = 0)""")
q("""CREATE MACRO fld_val(p) AS nullif(CASE WHEN strpos(p, '"') > 0 THEN substr(p, 2, length(p) - 2) ELSE p END, '')""")
q("""CREATE MACRO line_ok(parts, n) AS len(parts) = n AND list_bool_and(list_transform(parts, p -> fld_ok(p)))""")
# Days from civil date (proleptic Gregorian), valid for year >= 1.
q("""CREATE MACRO civ_days(y, m, d) AS (
      ((y - (m <= 2)::INT) // 400) * 146097
      + ((y - (m <= 2)::INT) % 400) * 365 + ((y - (m <= 2)::INT) % 400) // 4 - ((y - (m <= 2)::INT) % 400) // 100
      + (153 * (CASE WHEN m > 2 THEN m - 3 ELSE m + 9 END) + 2) // 5 + d - 1 - 719468)""")
q("""CREATE MACRO dim(y, m) AS CASE WHEN m = 2 THEN 28 + ((y % 4 = 0 AND y % 100 <> 0) OR y % 400 = 0)::INT
      WHEN m IN (4, 6, 9, 11) THEN 30 ELSE 31 END""")
q("""CREATE MACRO ts_calc(y, mo, d, h, mi, se, oh, om, sg) AS
      CASE WHEN y >= 1 AND mo BETWEEN 1 AND 12 AND d >= 1 AND d <= dim(y, mo) AND h <= 23 AND mi <= 59
                AND se <= 59 AND oh <= 23 AND om <= 59
           THEN (civ_days(y, mo, d)::BIGINT * 86400000 + h * 3600000 + mi * 60000 + se * 1000)
                - sg * (oh * 3600000 + om * 60000) END""")
# Timestamp -> epoch ms (BIGINT) or NULL when bad. F1 / F2 / F3 per rules.md.
q(r"""CREATE MACRO ts_ms(s) AS CASE
  WHEN s IS NULL THEN NULL
  WHEN regexp_full_match(s, '[0-9]+') THEN
       CASE WHEN length(ltrim(s, '0')) = 0 THEN 0::BIGINT
            WHEN length(ltrim(s, '0')) > 19 THEN NULL
            ELSE TRY_CAST(ltrim(s, '0') AS BIGINT) END
  WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-][0-9]{2}:[0-9]{2})') THEN
       ts_calc(substr(s,1,4)::INT, substr(s,6,2)::INT, substr(s,9,2)::INT,
               substr(s,12,2)::INT, substr(s,15,2)::INT, substr(s,18,2)::INT,
               CASE WHEN length(s) = 20 THEN 0 ELSE substr(s,21,2)::INT END,
               CASE WHEN length(s) = 20 THEN 0 ELSE substr(s,24,2)::INT END,
               CASE WHEN length(s) = 20 THEN 0 WHEN substr(s,20,1) = '-' THEN -1 ELSE 1 END)
  ELSE NULL END""")


def raw(folder, n, cols, name):
    """Parse every *.csv in folder per rules.md: drop header, malformed and empty lines."""
    path = os.path.join(DATA, folder)
    sel = ", ".join(f"fld_val(parts[{i + 1}]) AS {c}" for i, c in enumerate(cols))
    files = [f for f in os.listdir(path) if f.endswith(".csv")] if os.path.isdir(path) else []
    if not files:
        q(f"CREATE TABLE {name} AS SELECT " + ", ".join(f"NULL::VARCHAR AS {c}" for c in cols) + " WHERE false")
        return
    q(f"""CREATE TABLE {name} AS
          WITH lines AS (SELECT unnest((string_split(content, chr(10)))[2:]) AS line
                         FROM read_text('{path}/*.csv')),
               sp AS (SELECT string_split(line, ',') AS parts FROM lines)
          SELECT {sel} FROM sp WHERE line_ok(parts, {n})""")


def raw_file(file, n, cols, name):
    q(f"""CREATE TABLE {name} AS
          WITH lines AS (SELECT unnest((string_split(content, chr(10)))[2:]) AS line
                         FROM read_text('{os.path.join(DATA, file)}')),
               sp AS (SELECT string_split(line, ',') AS parts FROM lines)
          SELECT {", ".join(f"fld_val(parts[{i + 1}]) AS {c}" for i, c in enumerate(cols))}
          FROM sp WHERE line_ok(parts, {n})""")


with open(os.path.join(DATA, "manifest.json")) as f:
    manifest = json.load(f)
WS = q("SELECT ts_ms(?)", [manifest["window_start"]]).fetchone()[0]
WE = q("SELECT ts_ms(?)", [manifest["window_end"]]).fetchone()[0]
log(f"window {manifest['window_start']} .. {manifest['window_end']}")

raw_file("products.csv", 2, ["product_id", "category"], "products")
raw_file("campaigns.csv", 2, ["campaign_id", "channel"], "campaigns")
raw("links", 2, ["a", "b"], "links_raw")
raw("events", 7, ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"], "ev_raw")
raw("clicks", 4, ["click_id", "user_id", "campaign_id", "ts"], "ck_raw")
log("parsed")

# ---------- linked IDs: union-find, name = byte-wise smallest ID ----------
pairs = q("SELECT DISTINCT a, b FROM links_raw WHERE a IS NOT NULL AND b IS NOT NULL").fetchall()
parent = {}


def find(x):
    root = x
    while parent.get(root, root) != root:
        root = parent[root]
    while parent.get(x, x) != root:
        parent[x], x = root, parent[x]
    return root


def key(s):
    return s.encode("utf-8")


for a, b in pairs:
    parent.setdefault(a, a)
    parent.setdefault(b, b)
    ra, rb = find(a), find(b)
    if ra != rb:
        if key(ra) < key(rb):
            parent[rb] = ra
        else:
            parent[ra] = rb
# roots are always the byte-wise smallest ID of their set (we always attach the larger root under the smaller)
idmap = [(x, find(x)) for x in parent]
q("CREATE TABLE idmap (id VARCHAR, person VARCHAR)")
if idmap:
    import pyarrow as pa
    tbl = pa.table({"id": [i for i, _ in idmap], "person": [p for _, p in idmap]})
    q("INSERT INTO idmap SELECT * FROM tbl")
log(f"links: {len(pairs)} pairs, {len(idmap)} linked ids")

# ---------- events ----------
q("""CREATE TABLE ev_dedup AS
     SELECT event_id, arg_min({'u': user_id, 'ts': ts, 'et': event_type, 'p': product_id, 'pc': price_cents},
                              ts_ms(ingest_ts)) AS k, min(ts_ms(ingest_ts)) AS ing
     FROM ev_raw WHERE event_id IS NOT NULL GROUP BY event_id""")
q(f"""CREATE TABLE ev AS
      WITH v AS (SELECT k.u AS user_id, ts_ms(k.ts) AS ts, k.et AS event_type, k.p AS product_id, k.pc AS pc, ing
                 FROM ev_dedup)
      SELECT coalesce(m.person, v.user_id) AS person, v.ts, v.event_type, v.product_id,
             CASE WHEN v.event_type = 'purchase' THEN TRY_CAST(ltrim(v.pc, '0') AS BIGINT) END AS price
      FROM v LEFT JOIN idmap m ON m.id = v.user_id
      WHERE v.user_id IS NOT NULL AND v.ts IS NOT NULL AND v.ts >= {WS} AND v.ts < {WE}
        AND v.event_type IN ('view', 'cart', 'purchase') AND v.product_id IS NOT NULL
        AND (v.event_type <> 'purchase' OR (regexp_full_match(coalesce(v.pc, ''), '[0-9]+')
             AND length(ltrim(v.pc, '0')) BETWEEN 1 AND 8
             AND TRY_CAST(ltrim(v.pc, '0') AS BIGINT) BETWEEN 1 AND 10000000))
        AND v.ing - v.ts <= {DAY_MS}""")

# ---------- sessions ----------
q(f"""CREATE TABLE ev_s AS
      WITH f AS (SELECT *, CASE WHEN ts - lag(ts) OVER w > {GAP_MS} OR lag(ts) OVER w IS NULL THEN 1 ELSE 0 END AS nw
                 FROM ev WINDOW w AS (PARTITION BY person ORDER BY ts))
      SELECT *, sum(nw) OVER (PARTITION BY person ORDER BY ts ROWS UNBOUNDED PRECEDING) AS sid FROM f""")
q("""CREATE TABLE sess AS
     SELECT person, sid, min(ts) AS start, count(*) AS n_events,
            coalesce(sum(price) FILTER (WHERE event_type = 'purchase'), 0) AS revenue,
            count(*) FILTER (WHERE event_type = 'purchase') > 0 AS has_purchase
     FROM ev_s GROUP BY person, sid""")
q("ALTER TABLE sess ADD COLUMN day DATE")
q(f"UPDATE sess SET day = DATE '1970-01-01' + CAST(start // {DAY_MS} AS INTEGER)")

# ---------- clicks ----------
q(f"""CREATE TABLE ck AS
      WITH d AS (SELECT click_id, any_value(user_id) AS user_id, any_value(campaign_id) AS campaign_id,
                        any_value(ts) AS ts FROM ck_raw WHERE click_id IS NOT NULL GROUP BY click_id),
           v AS (SELECT click_id, user_id, campaign_id, ts_ms(ts) AS ts FROM d)
      SELECT coalesce(m.person, v.user_id) AS person, v.click_id, v.campaign_id, v.ts
      FROM v LEFT JOIN idmap m ON m.id = v.user_id
      WHERE v.user_id IS NOT NULL AND v.ts IS NOT NULL AND v.ts >= {WS - WEEK_MS} AND v.ts < {WE}
        AND v.campaign_id IN (SELECT campaign_id FROM campaigns)""")

# credit: latest click at or before start within 7 days; ties -> smallest click_id (byte-wise)
q("""CREATE TABLE ck_ts AS SELECT person, ts, arg_min(campaign_id, click_id) AS campaign_id
     FROM ck GROUP BY person, ts""")
q(f"""CREATE TABLE credit AS
      SELECT s.person, s.sid, s.day, s.revenue, s.has_purchase,
             CASE WHEN c.ts IS NOT NULL AND s.start - c.ts <= {WEEK_MS} THEN c.campaign_id ELSE 'organic' END AS campaign_id
      FROM sess s ASOF LEFT JOIN ck_ts c ON s.person = c.person AND s.start >= c.ts""")

# assists: merge each (person, campaign)'s clicks into [first, last + 7d] intervals; session assisted if start inside
q(f"""CREATE TABLE ivl AS
      WITH t AS (SELECT DISTINCT person, campaign_id, ts FROM ck),
           f AS (SELECT *, CASE WHEN ts - lag(ts) OVER w > {WEEK_MS} OR lag(ts) OVER w IS NULL THEN 1 ELSE 0 END AS nw
                 FROM t WINDOW w AS (PARTITION BY person, campaign_id ORDER BY ts)),
           g AS (SELECT *, sum(nw) OVER (PARTITION BY person, campaign_id ORDER BY ts ROWS UNBOUNDED PRECEDING) AS gi
                 FROM f)
      SELECT person, campaign_id, min(ts) AS lo, max(ts) + {WEEK_MS} AS hi FROM g GROUP BY person, campaign_id, gi""")
# Each interval covers a contiguous run of the person's sessions (sorted by start). Find that run's rank range with two
# ASOF joins, then split it by day using per-(person, day) rank ranges (<= 15 days per person). Intervals of one
# (person, campaign) are disjoint, so each (session, campaign) pair is counted at most once.
q("""CREATE TABLE sr AS SELECT person, start, day, row_number() OVER (PARTITION BY person ORDER BY start) AS r FROM sess""")
q("""CREATE TABLE pday AS SELECT person, day, min(r) AS dmin, max(r) AS dmax FROM sr GROUP BY person, day""")
q("""CREATE TABLE ivr AS
     WITH a AS (SELECT i.person, i.campaign_id, i.hi, s.r AS rlo FROM ivl i ASOF JOIN sr s
                ON i.person = s.person AND i.lo <= s.start)
     SELECT a.person, a.campaign_id, a.rlo, s.r AS rhi FROM a ASOF JOIN sr s ON a.person = s.person AND a.hi >= s.start""")
q("""CREATE TABLE assist AS
     SELECT v.campaign_id, d.day, sum(least(v.rhi, d.dmax) - greatest(v.rlo, d.dmin) + 1) AS assisted_sessions
     FROM ivr v JOIN pday d ON v.person = d.person AND d.dmax >= v.rlo AND d.dmin <= v.rhi
     WHERE v.rhi >= v.rlo
     GROUP BY v.campaign_id, d.day""")

# ---------- outputs ----------
os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)
q(f"""COPY (
      WITH ud AS (SELECT person, day, count(*) AS sessions, sum(n_events) AS events, sum(revenue) AS revenue_cents
                  FROM sess GROUP BY person, day),
           pc AS (SELECT e.person, s.day, coalesce(p.category, 'unknown') AS category, sum(e.price) AS amt
                  FROM ev_s e JOIN sess s USING (person, sid) LEFT JOIN products p ON p.product_id = e.product_id
                  WHERE e.event_type = 'purchase' GROUP BY ALL),
           top AS (SELECT person, day, category FROM pc
                   QUALIFY row_number() OVER (PARTITION BY person, day ORDER BY amt DESC, category::BLOB ASC) = 1)
      SELECT ud.person AS user_id, ud.day, ud.sessions::BIGINT AS sessions, ud.events::BIGINT AS events,
             ud.revenue_cents::BIGINT AS revenue_cents, top.category AS top_category
      FROM ud LEFT JOIN top USING (person, day)
    ) TO '{os.path.join(OUT, "user_daily", "part-0.parquet")}' (FORMAT parquet)""")
q(f"""COPY (
      WITH cr AS (SELECT campaign_id, day, count(*) AS sessions, count(*) FILTER (WHERE has_purchase) AS purchasing_sessions,
                         sum(revenue) AS revenue_cents FROM credit GROUP BY campaign_id, day),
           j AS (SELECT coalesce(cr.campaign_id, a.campaign_id) AS campaign_id, coalesce(cr.day, a.day) AS day,
                        coalesce(cr.sessions, 0) AS sessions, coalesce(cr.purchasing_sessions, 0) AS purchasing_sessions,
                        coalesce(cr.revenue_cents, 0) AS revenue_cents, coalesce(a.assisted_sessions, 0) AS assisted_sessions
                 FROM cr FULL OUTER JOIN assist a ON cr.campaign_id = a.campaign_id AND cr.day = a.day)
      SELECT j.campaign_id, CASE WHEN j.campaign_id = 'organic' THEN 'organic' ELSE c.channel END AS channel,
             j.day, j.sessions::BIGINT AS sessions, j.purchasing_sessions::BIGINT AS purchasing_sessions,
             j.revenue_cents::BIGINT AS revenue_cents, j.assisted_sessions::BIGINT AS assisted_sessions
      FROM j LEFT JOIN campaigns c ON c.campaign_id = j.campaign_id
    ) TO '{os.path.join(OUT, "campaign_daily", "part-0.parquet")}' (FORMAT parquet)""")
log("done")
