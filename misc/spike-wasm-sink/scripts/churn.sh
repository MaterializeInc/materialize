#!/usr/bin/env bash
# THROWAWAY: churn the table. Each statement updates 10k rows (20k diffs).
# Usage: churn.sh SECONDS [SLEEP]
end=$((SECONDS + $1)); i=0
while [ $SECONDS -lt $end ]; do
  psql -q -h localhost -p 6875 -U materialize -c "UPDATE t SET n = n + 1 WHERE id % 100 = $((i % 100))" >/dev/null 2>&1
  i=$((i + 1))
  sleep "${2:-0}"
done
echo "churn statements=$i"
