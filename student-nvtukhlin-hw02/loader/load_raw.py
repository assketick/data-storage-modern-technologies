import argparse
import csv
import io
import json
import sys
from pathlib import Path

from common import connect

DATA = Path("/opt/project/data")

EVENT_COLUMNS = [
    "time", "latitude", "longitude", "depth", "mag", "magtype", "nst", "gap", "dmin", "rms",
    "net", "id", "updated", "place", "type", "horizontalerror", "deptherror", "magerror",
    "magnst", "status", "locationsource", "magsource",
]
HISTORY_COLUMNS = ["net", "network_name", "valid_from", "valid_to"]

DDL = """
create schema if not exists raw;
create table if not exists raw.usgs_events (
    {columns},
    _source_system text,
    _slice_id text,
    _slice_order int,
    _row_num int,
    _is_synthetic boolean,
    _loaded_at timestamptz default now()
);
create table if not exists raw.network_history (
    net text,
    network_name text,
    valid_from text,
    valid_to text,
    _source_system text,
    _slice_id text,
    _slice_order int,
    _is_synthetic boolean,
    _loaded_at timestamptz default now()
);
""".format(columns=",\n    ".join(f"{c} text" for c in EVENT_COLUMNS))


def read_rows(path, expected):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = [h.strip().lower() for h in next(reader)]
        if header != expected:
            sys.exit(f"{path}: header {header} != {expected}")
        return list(reader)


def copy_rows(cur, table, columns, rows):
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerows(rows)
    buf.seek(0)
    cur.copy_expert(f"copy {table} ({', '.join(columns)}) from stdin with (format csv)", buf)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cases", default="")
    args = parser.parse_args()
    cases = [c.strip() for c in args.cases.split(",") if c.strip()]

    slices = [("baseline", DATA / "slice")] + [(c, DATA / "tests" / c) for c in cases]

    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute("truncate raw.usgs_events, raw.network_history")
            for order, (slice_id, folder) in enumerate(slices):
                if not folder.is_dir():
                    sys.exit(f"unknown slice: {slice_id}")
                meta = json.loads((folder / "meta.json").read_text())
                source_system = meta["source_system"]
                synthetic = meta["synthetic"]
                events = 0
                history = 0
                if "events_file" in meta:
                    rows = read_rows(folder / meta["events_file"], EVENT_COLUMNS)
                    tagged = [
                        r + [source_system, slice_id, order, n, synthetic]
                        for n, r in enumerate(rows, start=1)
                    ]
                    copy_rows(
                        cur, "raw.usgs_events",
                        EVENT_COLUMNS + ["_source_system", "_slice_id", "_slice_order", "_row_num", "_is_synthetic"],
                        tagged,
                    )
                    events = len(rows)
                if "history_file" in meta:
                    rows = read_rows(folder / meta["history_file"], HISTORY_COLUMNS)
                    tagged = [r + [source_system, slice_id, order, synthetic] for r in rows]
                    copy_rows(
                        cur, "raw.network_history",
                        HISTORY_COLUMNS + ["_source_system", "_slice_id", "_slice_order", "_is_synthetic"],
                        tagged,
                    )
                    history = len(rows)
                print(f"slice={slice_id} source_system={source_system} synthetic={synthetic} "
                      f"events={events} network_versions={history}")
            cur.execute("select count(*) from raw.usgs_events")
            print("raw.usgs_events rows:", cur.fetchone()[0])
    finally:
        conn.close()


if __name__ == "__main__":
    main()
