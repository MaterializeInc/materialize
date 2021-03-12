# Reading Compressed Objects from S3

## Summary

Users would like the ability to ingest compressed objects from S3, where the objects are
compressed using gzip.

## Goals

Users can create an S3 source, where the S3 bucket contains objects compressed using the gzip
algorithm, and Materialize can ingest those objects.

## Non-Goals

We are not planning to unify how S3 object and file objects are handled. While we will seek to
reuse code where possible, unifying S3 objects and file objects is beyond the scope for this
feature.

## Description

When the user creates a source, the user will be required to specify the compression algorithm
manually via the `COMPRESSION` specification:

    CREATE SOURCE ... FROM S3 OBJECTS ... COMPRESSION {NONE | GZIP | AUTO}

See [CREATE SOURCE ... FROM
FILE](https://materialize.com/docs/sql/create-source/text-file/#syntax) for an example of how
`COMPRESSION` is specified for files.

### Compression "NONE"

Materialize will not try to decompress any objects downloaded from this S3 source. Files that
cannot be parsed, either due to being compressed or for another reason, will result in errors in
the error stream for the source.

The `Content-Encoding` field will be ignored for functional purposes but verified. If the
`Content-Encoding` is not `identity`, a debug message will be generated indicating the mismatch.

### Compression "GZIP"

Materialize will use the gzip algorithm to decompress all objects downloaded from the S3 source.
Any files that fail to decompress will result in errors in the error stream for the source.

The `Content-Encoding` field will be ignored for functional purposes but verified. If the
`Content-Encoding` is not `gzip`, a debug message will be generated indicating the mismatch. A
mismatch on `Content-Encoding` cannot be a strict error, as there are many producers out there
that will uploaded compressed data without setting the `Content-Encoding` header, or the
`Content-Type` header, correctly.

### (Optional) Compression "AUTO"

Materialize will use the `Content-Encoding` field from object metadata to determine how to handle
the object.

#### Content-Encoding "identity"

The object will be treated as if it were specified with `COMPRESSION NONE`.

#### Content-Encoding "gzip"

The object will be treated as if it were specified with `COMPRESSION GZIP`.

#### No Content-Encoding Specified

Materialize will attempt to decompress the object using gzip, and if that fails, will attempt to
treat the file as uncompressed. Files that cannot be parsed, even after attempting decompression,
will result in errors in the error stream for the source.

## Additional Testing

Testing will be done via testdrive. See #6048 for an example on how to test object compression
with S3.

## Alternatives

### Using Content-Encoding

The first implementation of this feature tried to use the `Content-Encoding` header to
automatically determine which compression algorithm to use. However, it appears that few
applications set the header correctly ([Confluent S3
Connector](https://github.com/confluentinc/kafka-connect-storage-cloud/blob/e2c032b7976e28bafbef594b761c905c8f46ee21/kafka-connect-s3/src/main/java/io/confluent/connect/s3/storage/S3OutputStream.java#L198)
does not, for example). As such, we need to support adding flags to the source.

### Using Content-Type

Detect which compression algorithm based on the `Content-Type` field from the object metadata. For
example, `application/x-gzip` would be a gzip compressed object and `text/plain` would be an
object that is not compressed.

This solution is not ideal, as
[Type](https://www.w3.org/Protocols/rfc2616/rfc2616-sec7.html#sec7.2.1) is meant to specify the
media of the underyling data (such as `text/csv` or `text/plain`) and not the compression
algorithm applied. It should be noted that it is still possible that `Content-Type` can be a media
type that needs to be decompressed (see `application/x-gzip` example above).

### Using an Object Manifest - #5502

Require that the customer provide an object (or set of objects?) that indicate how each object
should handled.

### Using Object Name Suffix

Detect the compression algorithm based on the filename suffix, such as `.gz`.

### Reading Compression Headers / Magic Bytes

We could use automatic filetype detection, ala the `file` unix command, to determine how to decode
the file. At least one [magic crate](https://docs.rs/magic/0.12.2/magic/#usage-example) exists.
Magic works quite well for standard filetypes but would it work sufficiently for our purposes?

## Open questions

1. Do we try and add the `AUTO` compression mechanism or defer that until later?
