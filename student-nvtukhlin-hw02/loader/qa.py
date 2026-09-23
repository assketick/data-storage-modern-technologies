import sys

from common import connect

TABLES = {
    "mart": ("mart.daily_quakes", "event_date, class_key, source_system, net, network_name"),
    "fact": ("dds.fact_earthquake", "event_key"),
}


def snapshot(cur, name):
    cur.execute("create schema if not exists qa")
    for kind, (source, _) in TABLES.items():
        cur.execute(f"drop table if exists qa.{kind}_{name}")
        cur.execute(f"create table qa.{kind}_{name} as select * from {source}")
        cur.execute(f"select count(*) from qa.{kind}_{name}")
        print(f"snapshot qa.{kind}_{name}: {cur.fetchone()[0]} rows")


def compare(cur, a, b):
    ok = True
    for kind, (_, order) in TABLES.items():
        ta, tb = f"qa.{kind}_{a}", f"qa.{kind}_{b}"
        cur.execute(f"select count(*) from (select * from {ta} except select * from {tb}) x")
        only_a = cur.fetchone()[0]
        cur.execute(f"select count(*) from (select * from {tb} except select * from {ta}) x")
        only_b = cur.fetchone()[0]
        cur.execute(f"select count(*), md5(string_agg(t::text, '|' order by t::text)) from {ta} t")
        count_a, md5_a = cur.fetchone()
        cur.execute(f"select count(*), md5(string_agg(t::text, '|' order by t::text)) from {tb} t")
        count_b, md5_b = cur.fetchone()
        same = only_a == 0 and only_b == 0 and md5_a == md5_b and count_a == count_b
        ok = ok and same
        print(f"{kind}: rows {count_a} vs {count_b}; only_in_{a}={only_a}; only_in_{b}={only_b}; "
              f"md5 {md5_a} vs {md5_b}; identical={same}")
    print("RESULT:", "IDENTICAL" if ok else "DIFFERENT")
    return ok


def main():
    command = sys.argv[1]
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            if command == "snapshot":
                snapshot(cur, sys.argv[2])
            elif command == "compare":
                sys.exit(0 if compare(cur, sys.argv[2], sys.argv[3]) else 1)
            else:
                sys.exit(f"unknown command {command}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
