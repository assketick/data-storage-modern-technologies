import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BASELINE = ROOT.parent / "slice" / "usgs_20250303_20250306.csv"

with open(BASELINE, newline="") as f:
    reader = csv.DictReader(f)
    header = reader.fieldnames
    rows = list(reader)


def write_case(name, source_system, events=None, history=None):
    folder = ROOT / name
    folder.mkdir(exist_ok=True)
    meta = {"source_system": source_system, "synthetic": True}
    if events is not None:
        with open(folder / "events.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            writer.writerows(events)
        meta["events_file"] = "events.csv"
    if history is not None:
        with open(folder / "network_history.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["net", "network_name", "valid_from", "valid_to"])
            writer.writerows(history)
        meta["history_file"] = "network_history.csv"
    (folder / "meta.json").write_text(json.dumps(meta) + "\n")


earthquakes = [r for r in rows if r["type"] == "earthquake"]

dup = earthquakes[:3]
write_case("dup", "usgs", events=[dict(r) for r in dup])

minor_day3 = next(
    r for r in earthquakes
    if r["time"].startswith("2025-03-03") and 2.5 <= float(r["mag"]) < 4 and r["net"] == "us"
)
late = dict(minor_day3)
late["mag"] = "4.3"
late["updated"] = "2026-01-01T00:00:00.000Z"
write_case("late", "usgs", events=[late])

write_case(
    "overlap",
    "usgs",
    history=[["us", "USGS NEIC renamed (synthetic overlap)", "2025-03-04", "9999-12-31"]],
)

source_b = []
for i, r in enumerate(earthquakes[:3]):
    b = dict(r)
    b["net"] = "sb"
    b["time"] = r["time"][:11] + "12:00:0%d.000Z" % i
    b["mag"] = "3.%d" % (i + 1)
    b["place"] = "synthetic second source event %d" % i
    source_b.append(b)
write_case(
    "source_b",
    "synthetic_b",
    events=source_b,
    history=[["sb", "Synthetic second source network", "1900-01-01", "9999-12-31"]],
)

corrupt = dict(earthquakes[0])
corrupt["id"] = "corrupt_0001"
corrupt["mag"] = "99"
write_case("corrupt", "usgs", events=[corrupt])
