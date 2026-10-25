#!/usr/bin/env bash
# THROWAWAY: run SQL against local envd. Usage: q.sh 'SQL' | q.sh -f file.sql
# Port 6875 as materialize unless MZ_SYS=1 (6877 as mz_system).
if [ "${MZ_SYS:-0}" = 1 ]; then P=6877; U=mz_system; else P=6875; U=materialize; fi
if [ "$1" = "-f" ]; then
  psql -q -h localhost -p $P -U $U -v ON_ERROR_STOP=1 -At -f "$2" 2>&1 | grep -v -E "^ |NOTICE|^$|Issue a SQL|View documentation|Join our"
else
  psql -q -h localhost -p $P -U $U -v ON_ERROR_STOP=1 -At -c "$1" 2>&1 | grep -v -E "^ |NOTICE|^$|Issue a SQL|View documentation|Join our"
fi
