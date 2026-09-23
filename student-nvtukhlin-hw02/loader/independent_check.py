import csv
import sys
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from common import connect

SOURCE = Path(sys.argv[1] if len(sys.argv) > 1 else "/opt/project/data/slice/usgs_20250303_20250306.csv")
SEED = Path("/opt/project/dbt/seeds/mag_class.csv")

with open(SEED, newline="") as f:
    classes = [(r["class_name"], Decimal(r["mag_min"]), Decimal(r["mag_max"])) for r in csv.DictReader(f)]

expected = defaultdict(lambda: {"events": 0, "mag_sum": Decimal(0), "max": None})
with open(SOURCE, newline="") as f:
    for r in csv.DictReader(f):
        if r["type"] != "earthquake":
            continue
        mag = Decimal(r["mag"])
        name = next(n for n, lo, hi in classes if lo <= mag < hi)
        day = datetime.fromisoformat(r["time"].replace("Z", "+00:00")).date()
        cell = expected[(str(day), name)]
        cell["events"] += 1
        cell["mag_sum"] += mag
        cell["max"] = mag if cell["max"] is None else max(cell["max"], mag)

conn = connect()
with conn, conn.cursor() as cur:
    cur.execute(
        "select event_date::text, magnitude_class, sum(events), "
        "round(sum(avg_magnitude * events) / sum(events), 2), max(max_magnitude) "
        "from mart.daily_quakes group by 1, 2"
    )
    actual = {(d, c): (int(e), Decimal(a), Decimal(m)) for d, c, e, a, m in cur.fetchall()}
conn.close()

mismatches = 0
print(f"{'day':<12}{'class':<10}{'src_events':>11}{'mart_events':>12}{'src_avg':>9}{'mart_avg':>9}{'src_max':>8}{'mart_max':>9}")
for key in sorted(set(expected) | set(actual)):
    cell = expected.get(key)
    src = (cell["events"], (cell["mag_sum"] / cell["events"]).quantize(Decimal("0.01")), cell["max"]) if cell else None
    got = actual.get(key)
    ok = src is not None and got is not None and src[0] == got[0] and abs(src[1] - got[1]) <= Decimal("0.01") and src[2] == got[2]
    mismatches += 0 if ok else 1
    print(f"{key[0]:<12}{key[1]:<10}{(src[0] if src else '-'):>11}{(got[0] if got else '-'):>12}"
          f"{(src[1] if src else '-'):>9}{(got[1] if got else '-'):>9}{(src[2] if src else '-'):>8}{(got[2] if got else '-'):>9}"
          f"{'' if ok else '  <-- MISMATCH'}")
print("RESULT:", "MATCH" if mismatches == 0 else f"{mismatches} MISMATCHES")
sys.exit(1 if mismatches else 0)
