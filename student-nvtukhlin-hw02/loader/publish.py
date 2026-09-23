import argparse
from pathlib import Path

from common import connect

SQL = Path("/opt/project/sql/publish.sql")
LOCK_ID = 7331


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("select pg_advisory_xact_lock(%s)", (LOCK_ID,))
            cur.execute(SQL.read_text(), {
                "start_date": args.start_date,
                "end_date": args.end_date,
                "run_id": args.run_id,
            })
            cur.execute(
                "select rows_published, checksum from mart.publication_log "
                "where run_id = %s order by published_at desc limit 1",
                (args.run_id,),
            )
            rows, checksum = cur.fetchone()
            print(f"published run_id={args.run_id} window=[{args.start_date},{args.end_date}) "
                  f"rows={rows} checksum={checksum}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
