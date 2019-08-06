# Demo instructions

The following demonstrations assume that you've managed to install the Confluent
Platform installed and have started ZooKeeper, Kafka, and the Confluent Schema
Registry on their default ports. You can find setup instructions in the
[Developer guide](develop.md).

## Basic demo

Initialize using the demo-utils script

```bash session
$ source doc/demo-utils.sh
$ mtrz-startup
confluent is running
```

Then:

```bash session
$ mtrz-produce quotes '{
    "type": "record",
    "name": "envelope",
    "fields": [
        {
            "name": "before",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "row",
                    "fields": [{"name": "quote", "type": "string"}]
                }
            ]
        },
        {
            "name": "after",
            "type": [
                "null",
                "row"
            ]
        }
    ]
}'
# Type in some quotes. (There won't be a prompt. Type anyway.)
# {"before": null, "after": {"row": {"quote": "Syntax highlighting is juvenile. —Rob Pike"}}}
# {"before": null, "after": {"row": {"quote": "Arrogance in computer science is measured in nano-Dijkstras. —Alan Kay"}}}
# {"before": null, "after": null}  # send an empty record to flush
```

Then, in another shell:

```bash
$ psql -h localhost -p 6875 sslmode=disable
> CREATE SOURCE quotes FROM 'kafka://localhost/quotes' USING SCHEMA REGISTRY 'http://localhost:8081';
> PEEK quotes;
> CREATE MATERIALIZED VIEW business_insights AS SELECT quote, 42 FROM quotes;
> PEEK business_insights;
```

## Aggregate demo

```bash session
$ mtrz-produce aggdata '{
    "type": "record",
    "name": "envelope",
    "fields": [
        {
            "name": "before",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "row",
                    "fields": [{"name": "a", "type": "long"}, {"name": "b", "type": "long"}]
                }
            ]
        },
        {
            "name": "after",
            "type": [
                "null",
                "row"
            ]
        }
    ]
}'
{"before": null, "after": {"row": {"a": 1, "b": 1}}}
{"before": null, "after": {"row": {"a": 2, "b": 1}}}
{"before": null, "after": {"row": {"a": 3, "b": 1}}}
{"before": null, "after": {"row": {"a": 1, "b": 2}}}
{"before": null, "after": null}
```

```bash session
$ psql -h localhost -p 6875 sslmode=disable
> CREATE SOURCE aggdata FROM 'kafka://localhost/aggdata' USING SCHEMA REGISTRY 'http://localhost:8081';
> CREATE MATERIALIZED VIEW aggtest AS SELECT sum(a) FROM aggdata GROUP BY b;
> PEEK aggtest;
```

## Join demo

```bash session
# we need to use this schema twice
$ mtrz-produce src1 '{
    "type": "record",
    "name": "envelope",
    "fields": [
        {
            "name": "before",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "row",
                    "fields": [{"name": "a", "type": "long"}, {"name": "b", "type": "long"}]
                }
            ]
        },
        {
            "name": "after",
            "type": [
                "null",
                "row"
            ]
        }
    ]
}'
# {"before": null, "after": {"row": {"a": 1, "b": 1}}}
# {"before": null, "after": {"row": {"a": 2, "b": 1}}}
# {"before": null, "after": {"row": {"a": 1, "b": 2}}}
# {"before": null, "after": {"row": {"a": 1, "b": 3}}}

# in another terminal
$ mtrz-produce src2 '{
    "type": "record",
    "name": "envelope",
    "fields": [
        {
            "name": "before",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "row",
                    "fields": [{"name": "c", "type": "long"}, {"name": "d", "type": "long"}]
                }
            ]
        },
        {
            "name": "after",
            "type": [
                "null",
                "row"
            ]
        }
    ]
}'
# {"before": null, "after": {"row": {"c": 1, "d": 1}}}
# {"before": null, "after": {"row": {"c": 1, "d": 2}}}
# {"before": null, "after": {"row": {"c": 1, "d": 3}}}
# {"before": null, "after": {"row": {"c": 3, "d": 1}}}
# {"before": null, "after": null}

# now go back to src1 and
# {"before": null, "after": null}
```

```bash session
$ psql -h localhost -p 6875 sslmode=disable
> CREATE SOURCE src1 FROM 'kafka://localhost/src1' USING SCHEMA REGISTRY 'http://localhost:8081';
> CREATE SOURCE src2 FROM 'kafka://localhost/src2' USING SCHEMA REGISTRY 'http://localhost:8081';
> CREATE MATERIALIZED VIEW jointest AS SELECT a, b, d FROM src1 JOIN src2 ON c = b;
> PEEK jointest;
```
