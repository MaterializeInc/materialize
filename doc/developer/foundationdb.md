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
* On Linux we link against `libfdb_c` unconditionally.
  On other systems, enable the `fdb` feature to link against FoundationDB.

## Integration with Materialize

Materialize can use FoundationDB to store consensus and timestamp oracle data, the only two metadata components required for Materialize.

### Consensus

Consensus offers an interface where keys are mapped to values identified by sequence numbers.
Sequence numbers are integers that increase monotonically, ensuring that updates to a key are ordered.
We map the consensus structure to the following schema in FoundationDB:

* Looking up data by key and sequence number:
    ```
    ./data/<key>/<sequence_number> -> <value>
    ```
    The `data` directory contains entries for each key and sequence number pair, mapping to the corresponding value.
    The latest sequence number for a key is determined by a reverse scan of the data entries, which ensures locality between the current sequence number lookup and the actual data.
* The `keys` directory contains entries for each key to simplify listing all keys:
    ```
    ./keys/<key> -> []
    ```

### Timestamp Oracle

The timestamp oracle tracks the read and write timestamps for timelines.
We map the timestamp oracle structure to the following schema in FoundationDB:

* Looking up the read timestamp for a timeline:
    ```
    ./<timeline>/read_ts -> <timestamp>
    ```
    For each timeline, the `read_ts` entry maps to the latest read timestamp.
* Looking up the write timestamp for a timeline:
    ```
    ./<timeline>/write_ts -> <timestamp>
    ```
    For each timeline, the `write_ts` entry maps to the latest write timestamp.

## Running tests against FoundationDB

Set the `EXTERNAL_METADATA_STORE` to `foundationdb` to force all compatible tests to use FoundationDB as the metadata store.

## Handling FoundationDB shutdown

FoundationDB has a peculiar way to handle its lifecycle.
According to the documentation, one needs to initialize the network _once_ per process.
Before shutdown, one needs to stop the network.
A stopped network cannot be restarted.

For Materialize itself (environmentd, clusterd) doesn't have a clean exit path and always terminates without running exit handlers.
This means that we don't need to worry about stopping the network cleanly.
However, tests do have a clean exit path, and we need to ensure that we either stop the network, or terminate without stopping the network.

Rust tests do not have a notion of global setup and teardown, which makes it difficult to manage the FoundationDB network lifecycle.
To work around this, we use the `ctor` crate to terminate the process without dropping the network and without running destructors.

Specifically, this means:
* Tests using FoundationDB need to depend on `mz-foundationdb` with the `shutdown` feature.
  This ensures that we register an exit handler that terminates immediately.
