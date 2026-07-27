Unlike Avro, Protobuf does not serialize a schema with the message, so Materialize expects:

* A `FileDescriptorSet` that encodes the Protobuf message schema. You can generate the `FileDescriptorSet` with [`protoc`](https://grpc.io/docs/protoc-installation/), for example:

  ```shell
  protoc --include_imports --descriptor_set_out=SCHEMA billing.proto
  ```

* A top-level message name and its package name, so Materialize knows which message from the `FileDescriptorSet` is the top-level message to decode, in the following format:

  ```shell
  <package name>.<top-level message>
  ```

  For example, if the `FileDescriptorSet` were from a `.proto` file in the
    `billing` package, and the top-level message was called `Batch`, the
    _message&lowbar;name_ value would be `billing.Batch`.

#### Schema versioning

The _latest_ schema is retrieved using the [`TopicNameStrategy`](https://docs.confluent.io/current/schema-registry/serdes-develop/index.html) strategy at the time the source or table is created.

#### Schema evolution

As long as the `.proto` schema definition changes in a [compatible way](https://developers.google.com/protocol-buffers/docs/overview#updating-defs), Materialize will continue using the original schema definition by mapping values from the new to the old schema version. To pick up the new version of the schema with the legacy syntax (`CREATE SOURCE ... FORMAT PROTOBUF ...`), you need to **drop and recreate** the source. With the new syntax (`CREATE SOURCE` plus [`CREATE TABLE ... FROM SOURCE`](/sql/create-table/)), you can instead create a new table that reads the evolved schema and cut over without downtime, following the approach in [Handle upstream schema changes with zero downtime](/ingest-data/kafka/source-versioning/).

#### Supported types

Materialize supports all [well-known](https://developers.google.com/protocol-buffers/docs/reference/google.protobuf) Protobuf types from the `proto2` and `proto3` specs, _except for_ recursive `Struct` values and map types.

#### Multiple message schemas

When using a schema registry with Protobuf sources, the registered schemas must contain exactly one `Message` definition.
