# Sources

Learn about sources in Materialize.



## Overview

Sources describe external systems you want Materialize to read data from, and
provide details about how to decode and interpret that data. A simplistic way to
think of this is that sources represent streams and their schemas; this isn't
entirely accurate, but provides an illustrative mental model.

In terms of SQL, sources are similar to a combination of tables and
clients.

- Like tables, sources are structured components that users can read from.
- Like clients, sources are responsible for reading data. External
  sources provide all of the underlying data to process.

By looking at what comprises a source, we can develop a sense for how this
combination works.

[//]: # "TODO(morsapaes) Add details about source persistence."

## Source components

Sources consist of the following components:

Component      | Use                                                                                               | Example
---------------|---------------------------------------------------------------------------------------------------|---------
**Connector**  | Provides actual bytes of data to Materialize                                                      | Kafka
**Format**     | Structures of the external source's bytes, i.e. its schema                                        | Avro
**Envelope**   | Expresses how Materialize should handle the incoming data + any additional formatting information | Upsert

### Connectors

Materialize bundles native connectors for the following external systems:

<div class="multilinkbox">
<div class="linkbox ">
  <div class="title">
    Databases (CDC)
  </div>
  <ul>
<li><a href="/ingest-data/postgres/" >PostgreSQL</a></li>
<li><a href="/ingest-data/mysql/" >MySQL</a></li>
<li><a href="/ingest-data/sql-server/" >SQL Server</a></li>
<li><a href="/ingest-data/cdc-cockroachdb/" >CockroachDB</a></li>
<li><a href="/ingest-data/mongodb/" >MongoDB</a></li>
</ul>

</div>

<div class="linkbox ">
  <div class="title">
    Message Brokers
  </div>
  <ul>
<li><a href="/ingest-data/kafka/" >Kafka</a></li>
<li><a href="/sql/create-source/kafka" >Redpanda</a></li>
</ul>

</div>

<div class="linkbox ">
  <div class="title">
    Webhooks
  </div>
  <ul>
<li><a href="/ingest-data/webhooks/amazon-eventbridge/" >Amazon EventBridge</a></li>
<li><a href="/ingest-data/webhooks/segment/" >Segment</a></li>
<li><a href="/sql/create-source/webhook" >Other webhooks</a></li>
</ul>

</div>

</div>



For details on the syntax, supported formats and features of each connector, check out the dedicated `CREATE SOURCE` documentation pages.


## Sources and clusters

Sources require compute resources in Materialize, and so need to be associated
with a [cluster](/concepts/clusters/). If possible, dedicate a cluster just for
sources.

See also [Operational guidelines](/manage/operational-guidelines/).

## Related pages

- [`CREATE SOURCE`](/sql/create-source)
