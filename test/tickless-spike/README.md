# Tickless frontiers spike harness

Measures end-to-end freshness and strict serializable read latency of a
PostgreSQL source under the event-driven binding flag.

## Setup

    docker run -d --name tickless-pg -p 5434:5432 -e POSTGRES_PASSWORD=postgres postgres:16 -c wal_level=logical
    docker start cockroach || docker run --name=cockroach -d -p 26257:26257 -p 26258:8080 cockroachdb/cockroach:latest start-single-node --insecure --store=type=mem,size=2G
    bin/environmentd --reset

## Run

    bin/pyactivate -m pip install psycopg   # only if missing
    bin/pyactivate test/tickless-spike/spike.py --duration 30 --rate 20

Each mode drops and recreates the source so that dyncfg changes take effect
at operator build time.
