# Prometheus Metrics From Queries

## The Problem

Users want to be able to monitor their Materialize workloads and data products.
Setting up external tools to convert SQL queries into prometheus metrics is labor intensive, error prone, and often buggy.

This also applies to Materialize Cloud, as we run our own external SQL exporter which could be removed if this could be hosted by environmentd.

## Success Criteria

- Users can define SQL queries that get turned into prometheus metrics.
- Users can group these metrics into HTTP endpoints, so they may have separate scrape configs (for different auth requirements and/or scrape frequency).
- Materialize Cloud can remove the promsql exporter.

## Out of Scope

- Generic HTTP endpoint creation for formats other than Prometheus.

    While the proposed solution could easily be extended for other API types, that is not required for this to work for prometheus.

## Solution Proposal

Allow users to create HTTP endpoints in SQL with custom prometheus metrics.

```sql
CREATE API mydatabase.myschema.myprometheus FORMAT PROMETHEUS ON CLUSTER "mycluster" ON LISTENER "external";
```
This will create an HTTP endpoint at `/metrics/custom/mydatabase/myschema/myprometheus` on the "external" HTTP listener (as named in the listeners configmap), and a (currently empty) table with the following schema:
```
metric_name TEXT,
metric_type TEXT,
help TEXT,
database TEXT,
schema TEXT,
view TEXT,
value_column_name TEXT
```

The `metric_name`, `metric_type`, and `help` fields describe the prometheus metric itself.

The `database`, `schema`, and `view` fields are a reference to a view containing the metric data. The `value_column_name` is the name of a column in that view which contains the value of the metric. All other columns in the view will be used as labels.

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
INSERT INTO mydatabase.myschema.myprometheus
VALUES      ('leads',
             'gauge',
             'Count of leads and whether they have been converted',
             'mydatabase',
             'myschema',
             'converted_leads',
             'count')
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
