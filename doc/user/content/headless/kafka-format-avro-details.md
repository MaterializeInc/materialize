#### Schema registries

Materialize can retrieve Avro schemas from either of two schema registries,
selected by the `USING` clause:

- **[Confluent Schema Registry](/sql/create-connection/#confluent-schema-registry)**
  (`USING CONFLUENT SCHEMA REGISTRY`): schemas are looked up by topic using the
  `TopicNameStrategy`, and the message's embedded schema ID resolves the writer
  schema at decode time.

- **[AWS Glue Schema Registry](/sql/create-connection/#aws-glue-schema-registry)**
  (`USING AWS GLUE SCHEMA REGISTRY`) {{< private-preview-inline />}}: schemas are
  looked up by the `SCHEMA NAME` you provide, and the message's embedded schema
  version ID resolves the writer schema at decode time. Each `FORMAT AVRO USING
  AWS GLUE` clause resolves a single schema. To decode keys and values from
  different schemas (for example, under `ENVELOPE UPSERT` or `ENVELOPE
  DEBEZIUM`), you must specify `KEY FORMAT ... VALUE FORMAT ...` explicitly.

#### Schema versioning

The schema is resolved when the source or table is created. With
[Confluent Schema Registry](/sql/create-connection/#confluent-schema-registry),
the _latest_ schema is retrieved using the
[`TopicNameStrategy`](https://docs.confluent.io/current/schema-registry/serdes-develop/index.html)
strategy. With [AWS Glue Schema
Registry](/sql/create-connection/#aws-glue-schema-registry), the latest version
of the schema named by `SCHEMA NAME` is retrieved.

#### Schema evolution

As long as the writer schema changes in a [compatible way](https://avro.apache.org/docs/++version++/specification/#schema-resolution), Materialize will continue using the original reader schema definition by mapping values from the new to the old schema version. This applies to both Confluent Schema Registry and AWS Glue Schema Registry.

To pick up the new version of the writer schema, the approach depends on the syntax you used:

- **Legacy syntax** (`CREATE SOURCE ... FORMAT AVRO ...`): you need to **drop and recreate** the source, which incurs downtime.
- **New syntax** (`CREATE SOURCE` plus [`CREATE TABLE ... FROM SOURCE`](/sql/create-table/)): you can create a new table that reads the evolved schema and cut over without downtime. See [Handle upstream schema changes with zero downtime](/ingest-data/kafka/source-versioning/).

#### Name collision

To avoid [case-sensitivity](/sql/identifiers/#case-sensitivity) conflicts with Materialize identifiers, we recommend double-quoting all field names when working with Avro-formatted sources.

#### Supported types

Materialize supports all [Avro
types](https://avro.apache.org/docs/++version++/specification/), _except for_
recursive types and union types in arrays.
