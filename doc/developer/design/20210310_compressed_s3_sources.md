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

### Compression "GZIP"

Materialize will use the gzip algorithm to decompress all objects downloaded from the S3 source.
Any files that fail to decompress will result in errors in the error stream for the source.

### (Optional) Compression "AUTO"

Materialize will use the `Content-Encoding` filed from object metadata to determine how to handle
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

The first implementation of this feature tried to use the `Content-Encoding` header to
automatically determine which compression algorithm to use. However, it appears that few
applications set the header correctly ([Confluent S3
Connector](https://github.com/confluentinc/kafka-connect-storage-cloud/blob/e2c032b7976e28bafbef594b761c905c8f46ee21/kafka-connect-s3/src/main/java/io/confluent/connect/s3/storage/S3OutputStream.java#L198)
does not, for example). As such, we need to support adding flags to the source.

## Open questions

1. Do we try and add the `AUTO` compression mechanism or defer that until later?
