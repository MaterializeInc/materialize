# FoundationDB Developer Guide

Materialize supports using FoundationDB as a metadata storage layer.
This document outlines how we integrate with FoundationDB.

## Overview of FoundationDB

FoundationDB offers a key-value interface where keys and values are arbitrary byte strings.
Keys follow a directory structure, allowing for hierarchical organization of data.
FoundationDB provides ACID transactions, ensuring data integrity and consistency.

## Installation

FoundationDB requires some setup to the host system.
Please refer to the [official FoundationDB documentation](https://apple.github.io/foundationdb/) for installation instructions.

* On Linux, install the official packages, at least the client.
* On macOS, we try not to link against FoundationDB as it requires manual setup.
  However, you can install it from the releases, and then set `LIBRARY_PATH=/usr/local/lib` to link againt the FoundationDB client library.

## Integration with Materialize

Materialize can use FoundationDB to store consensus and timestamp oracle data, the only two metadata compontents required for Materialize.

### Consensus

Consensus offers an interface where keys are mapped to values identified by sequence numbers.
Sequence numbers are integers that increase monotonically, ensuring that updates to a key are ordered.
We map the consensus structure to the following schema in FoundationDB:

* Looking up the latest sequence number for a key:
    ```
    ./seqno/<key> -> <sequence_number>
    ```
  The `seqno` directory contains entries for each key, mapping to their latest sequence number.
* Looking up data for a key and a sequence number:
    ```
    ./data/<key>/<sequence_number> -> <value>
    ```
  The `data` directory contains entries for each key and sequence number pair, mapping to the corresponding value.

### Timestamp Oracle

The timestamp oracle tracks the read and write timestamps for timelines.
We map the timestamp oracle structure to the following schema in FoundationDB:

* Looking up the read timestamp for a timeline:
    ```
    ./read_ts/<timeline> -> <timestamp>
    ```
  The `read_ts` directory contains entries for each timeline, mapping to their read timestamps.
* Looking up the write timestamp for a timeline:
    ```
    ./write_ts/<timeline> -> <timestamp>
    ```
  The `write_ts` directory contains entries for each timeline, mapping to their write timestamps.

## Running tests against FoundationDB

Set the `EXTERNAL_METADATA_STORE` to `foundationdb` to force all compatible tests to use FoundationDB as the metadata store.
