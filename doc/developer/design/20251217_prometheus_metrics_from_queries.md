# Prometheus Metrics From Queries

## The Problem

Users want to be able to monitor their Materialize workloads and data products.
Setting up external tools to convert SQL queries into prometheus metrics is labor intensive, error prone, and often buggy.

## Success Criteria

- Users can define SQL queries that get turned into prometheus metrics.
- Users can group these metrics into HTTP endpoints, so they may have separate scrape configs (for different auth requirements and/or scrape frequency).

## Out of Scope

- Generic HTTP endpoint creation for formats other than Prometheus.

    While the proposed solution could easily be extended for other API types, that is not required for this to work for prometheus.

- Removal of the Materialize Cloud promsql exporter.

    The promsql exporter relies on internal tables which cannot have views made from them, which would complicate this proposal.
    We should just move all those queries into normal metrics endpoints instead. That way, customers can also get access to these metrics.

    There are two open tickets related to this:
    - https://github.com/MaterializeInc/database-issues/issues/10028
    - https://github.com/MaterializeInc/database-issues/issues/10030

## Solution Proposal

Allow users to create HTTP endpoints in SQL with custom prometheus metrics.

```sql
CREATE API mydatabase.myschema.myprometheus FORMAT PROMETHEUS IN CLUSTER "mycluster";
```
This will create an HTTP endpoint at `/api/metrics/custom/mydatabase/myschema/myprometheus` on all HTTP listeners with the `endpoint_api` enabled in the listeners configmap.

This new api object would be added to a system table `mz_apis` for later reference:
```
id TEXT,
oid OID,
schema_id TEXT,
name TEXT,
cluster_id TEXT,
owner_id TEXT,
privileges mz_aclitem[]
```

The `cluster_id` references the cluster used to peek the metric source relations (corresponds to `mz_clusters.id`).

Users can then add metrics to that endpoint using SQL commands:
```sql
CREATE METRIC <name>
IN API <api>
AS (TYPE <prometheus_type>,
    HELP <help_text>,
    SERIES FROM <reference_to_view>,
    VALUE COLUMN <name_of_value_column>);
```

This will add a new metric object to a system table `mz_metrics` for later reference:
```
id TEXT,
oid OID,
schema_id TEXT,
name TEXT,
api_id TEXT,
type TEXT,
help TEXT,
series_from TEXT,
value_column TEXT,
owner_id TEXT
```

The `name`, `type`, and `help` fields describe the prometheus metric itself. `api_id` references the `mz_apis` entry the metric is attached to.

The `series_from` field is the ID of the relation containing the metric data (corresponds to `mz_catalog.mz_relations.id`). The `value_column` is the name of a column in that relation which contains the value of the metric. All other columns in the relation will be used as labels.

An example metric view:
```sql
 CREATE VIEW converted_leads
AS
  (SELECT Count(*),
          converted
   FROM   (SELECT id,
                  CASE
                    WHEN converted_at IS NULL THEN 'FALSE'
                    ELSE 'TRUE'
                  END AS converted
           FROM   leads)
   GROUP  BY converted);
```

This might look like:
| count | converted |
|-------|-----------|
|22|TRUE|
|67|FALSE|

The user can then add this metric to their registry:
```sql
CREATE METRIC leads
IN API mydatabase.myschema.myprometheus
AS (TYPE 'gauge',
    HELP 'Count of leads and whether they have been converted',
    SERIES FROM mydatabase.myschema.converted_leads,
    VALUE COLUMN 'count');
```

When querying the HTTP endpoint at `/api/metrics/custom/mydatabase/myschema/myprometheus`, they would then get a response like:
```
# HELP mz_custom_leads Count of leads and whether they have been converted
# TYPE mz_custom_leads gauge
mz_custom_leads{converted="TRUE"} 22
mz_custom_leads{converted="FALSE"} 67
```

All exposed metric names are prefixed with `mz_custom_` to namespace user-defined metrics and avoid collisions with Materialize's built-in metrics. The prefix is injected at exposition time; the user-supplied metric name (e.g. `leads`) is what appears in `CREATE METRIC` and the `mz_metrics` catalog.

## RBAC

Scrapes do **not** run as the API owner. Each request to `/api/metrics/custom/...` runs as the role that authenticated the HTTP request, exactly as if that role had issued the underlying `SELECT`s itself. On listeners with `authenticator_kind = "None"`, the role is taken from the basic-auth userinfo (the password is not checked); if no username is supplied, the request falls back to the built-in `anonymous_http_user` role.

For a scrape to succeed, the scraping role must hold:

- `USAGE` on the API object,
- `USAGE` on the API's cluster (the cluster used to peek the metric relations), and
- `SELECT` on every relation referenced by a metric's `SERIES FROM` (and the `USAGE` on the containing database/schema that `SELECT` already requires).

When a permission is missing, the endpoint fails the whole scrape rather than silently omitting metrics (a partial exposition would otherwise look like a healthy target reporting zero):

- Missing `USAGE` on the API → `404 Not Found`. The API is resolved from the catalog and gated before any query runs; returning `404` rather than `403` means the role cannot distinguish "API exists but you can't see it" from "no such API", matching how Materialize hides objects a role has no access to.
- Missing `USAGE` on the cluster, or missing `SELECT` on any metric's `SERIES FROM` relation → `403 Forbidden`. Both surface from executing the scrape query as the scraping role (SQLSTATE `42501`, insufficient privilege), so they are not distinguished from each other.

The API-`USAGE` check reuses the same RBAC gating predicate as `CREATE`-time validation (`is_rbac_enforced_for_session`), so scrape-time checks cannot drift from the rules applied when the object was defined, and the cluster/relation checks fall out of running the query itself. This also means the usual escape hatches apply: superusers, system roles, and environments with RBAC disabled bypass these checks.

## Minimal Viable Prototype

- [Hackathon presentation from May 2025](https://docs.google.com/presentation/d/1ek0tOlECHfpoBp_-vtcDWhN4YHpaWBfRuQyENGFbWLw/edit?slide=id.g35c518b4039_14_3503#slide=id.g35c518b4039_14_3503)
- [Hackathon code from May 2025](https://github.com/MaterializeInc/materialize/compare/main...alex-hunt-materialize:materialize:external_api)
- [Hackathon brainstorming from May 2025](https://www.notion.so/materialize/Hackathon-Alex-Justin-1f913f48d37b805e88b0e25a8ad1a763)

While not the exact same interface, it captures the idea proposed here.

## Alternatives

External SQL exporters.

We currently use one of our own in Materialize Cloud, which we wrote after we hit numerous problems with third-party ones. We currently still recommend third-party solutions to our customers, which is not ideal.

## Open questions

- Exact syntax and SQL object types. We might want to have dedicated SQL syntax for creating metrics, or have some dedicated reference to the views rather than text fields, for example.
- Should we require indexed or materialized views?
