# S3 Sources with Headers

## Summary

<!--
// Brief, high-level overview. A few sentences long.
// Be sure to capture the customer impact - framing this as a release note may be useful.
-->

S3 and in the future, multi-file, [GCS][], and other sources have multiple
independent objects that may have their own headers.

For example, imagine an S3 bucket that has historical data encoded as CSV
formatted files, one file per day. Each CSV file has its own header line, which
is semantically meaningful but should not be included as data rows.

This feature adds support for decoding multiple CSV objects without garbage from
headers making it into query results, and lays the groundwork for future file
types (e.g. Avro OCF, Parquet) that all require their own header lines.

[GCS]: https://cloud.google.com/storage/docs/

## Goals

<!--
// Enumerate the concrete goals that are in scope for the project.
-->

Supporting decoding multiple CSV headers within a framework that will function
for future multi-object sources.

## Non-Goals

<!--
// Enumerate potential goals that are explicitly out of scope for the project
// ie. what could we do or what do we want to do in the future - but are not doing now
-->

## Description

<!--
// Describe the approach in detail. If there is no clear frontrunner, feel free to list all approaches in alternatives.
// If applicable, be sure to call out any new testing/validation that will be required
-->

User-facing changes are twofold:

* The ability to specify additional formats (e.g `FORMAT CSV WITH HEADER`)
* The ability to specify a header or schema inline as part of the declaration

When a user specifies a source format that has a header, we will obtain an
arbitrary file from the source declaration and inspect its header, making it the
cannonical schema. All files must have exactly the same schema -- if any are
encountered that do not match, the dataflow will be put into an error state.

Internally, there are only a few changes that need to be made.

* Add some code in purify that can obtain an object according to the source
  declaration and use it to extract the schema
* Modify the messages sent from sources to include a signifier for "beginning of
  object", which will cause the decode loop to perform schema validation and
  drop the header line.

## Alternatives and future work

<!--
// Similar to the Description section. List of alternative approaches considered, pros/cons or why they were not chosen
-->

### Supporting a subset of schema fields

For some use cases -- imagine an ETL job that has added or dropped columns over
time -- it would be useful to support only a few required fields, or allowing a
subset or reordering of fields.

Supporting this is not planned for the initial implementation, but may be
considered if it is requested.

### Lazy sources

For S3 sources, it's possible to specify that the *only* objects to ingest are
the ones specified in an SQS queue. For this use case, reading from the SQS
queue in order to determine what the header should be would be destructive.

Currently, we intend to require that for this case the schema or header be
included literally in the create source declaration.

## Open questions

<!--
// Anything currently unanswered that needs specific focus. This section may be expanded during the doc meeting as
// other unknowns are pointed out.
// These questions may be technical, product, or anything in-between.
-->
