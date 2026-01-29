# Prometheus Metrics From Queries

## The Problem

Users want to be able to monitor their Materialize workloads and data products.
Setting up external tools to convert SQL queries into prometheus metrics is labor intensive, error prone, and often buggy.

This also applies to Materialize Cloud, as we run our own external SQL exporter which could be removed if this could be hosted by environmentd.

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
CREATE API mydatabase.myschema.myprometheus FORMAT PROMETHEUS ON CLUSTER "mycluster" ON LISTENER "external";
```
This will create an HTTP endpoint at `/metrics/custom/mydatabase/myschema/myprometheus` on the "external" HTTP listener (as named in the listeners configmap).

This new api object would be added to a system table `mz_apis` for later reference.

Users can then add metrics to that endpoint using SQL commands:
```sql
CREATE METRIC <name>
IN API <api>
AS (TYPE <prometheus_type>,
    HELP <help_text>,
    VALUES_FROM <reference_to_view>,
    VALUE_COLUMN <name_of_value_column>);
```

This will add a new metric object to a system table `mz_metrics` for later reference:
```
metric_name TEXT,
metric_type TEXT,
help TEXT,
values_from TEXT,
value_column_name TEXT
```

The `metric_name`, `metric_type`, and `help` fields describe the prometheus metric itself.

The `values_from` field is a reference to a view containing the metric data. The `value_column_name` is the name of a column in that view which contains the value of the metric. All other columns in the view will be used as labels.

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
    VALUES_FROM mydatabase.myschema.converted_leads,
    VALUE_COLUMN 'count');
```

When querying the HTTP endpoint at `/metrics/custom/mydatabase/myschema/myprometheus`, they would then get a response like:
```
# HELP leads Count of leads and whether they have been converted
# TYPE leads gauge
leads{converted="TRUE"} 22
leads{converted="FALSE"} 67
```

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
- How to specify which listener a metrics endpoint is on. Users don't define the listeners configmap, so referencing the names we define there seems a bit weird.
- Should we require indexed or materialized views?
