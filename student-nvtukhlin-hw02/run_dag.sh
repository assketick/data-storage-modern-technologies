#!/usr/bin/env bash
set -u
RUN_ID="$1"
CASES="${2:-}"
START="${3:-2025-03-03}"
END="${4:-2025-03-06}"

airflow() { docker compose exec -T airflow-scheduler airflow "$@" 2>/dev/null; }

airflow dags trigger quakes_elt --run-id "$RUN_ID" \
  --conf "{\"cases\": \"$CASES\", \"start_date\": \"$START\", \"end_date\": \"$END\"}" >/dev/null

STATE=none
for _ in $(seq 1 90); do
  STATE=$(airflow dags list-runs -d quakes_elt -o table | awk -F'|' -v r="$RUN_ID" '{gsub(/ /,"",$2); gsub(/ /,"",$3)} $2==r {print $3}')
  if [ "$STATE" = "success" ] || [ "$STATE" = "failed" ]; then break; fi
  sleep 4
done

echo "run_id=$RUN_ID cases='$CASES' window=[$START,$END) dag_state=${STATE:-none}"
TABLE=$(airflow tasks states-for-dag-run quakes_elt "$RUN_ID")
for t in load_raw dbt_build_candidate dbt_test publish; do
  echo "$TABLE" | awk -F'|' -v t="$t" '{gsub(/ /,"",$3); gsub(/ /,"",$4)} $3==t {printf "  %-22s %s\n", $3, $4}'
done
