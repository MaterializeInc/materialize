# `ALTER CONNECTION`

- Associated:
    - https://github.com/MaterializeInc/materialize/issues/17620

## Context

A `CONNECTION` must be valid for a source to ingest data--however, in our
current scheme, `CONNECTION`s are immutable so any change to the service hosting
the source permanently wedges the source in Materialize. This is undesirable
because the `DROP`/`CREATE` cycle for a source can be very painful for users
because of rehydration.

## Goals

Allow users to modify a `CONNECTION` s/t they can repair the TCP connection
between MZ and their services.

## Non-Goals

Allow users to meticulously control their sources' state changes.

## Overview

We can perform a process similar to `ALTER SOURCE` wherein we update some state
in memory, re-plan something, update it in the catalog, and then communicate the
updated state throughout the system.

## Detailed description

1. Parse `ALTER CONNECTION <name> AS (KAFKA|POSTGRES) SET <option> =? <val>`.
    - In parsing `AS` clause dictates the options available. These will get
      unified in an enum that passes from parsing into planning.
1. During planning/purification:
    1. Check if `AS` clause matches existing connection type.
    1. Perform connection validation using same logic as `CREATE CONNECTION`.
1. During sequencing:
    1. Load the connection in memory. Update its values.
    1. Re-plan the value to get its updated `CREATE CONNECTION` string, just as
       we do for `ALTER SOURCE`.
    1. Commit a catalog update op for the connection.
    1. Find all sources that use the connection and update their ingestion
       description to use the new connection. I believe this only needs to be
       done to the in-memory version of the `SOURCE` because the on-disk
       representation only contains the connection details by name.
    1. To the storage controller, send a `RunIngestion` command with the new,
       in-memory representation of the ingestion.

       This will restart all of the sources with the new connection state.

## Alternatives

- `ALTER SOURCE...SET CONNECTION` would let us swap out one connection object
  for another. This would let users create new connections, validate that they
  work, and then swap them over one-by-one.

  The advantage here is being able to more meticulously control the state
  changes sources encounter. The downside is that it has to be done
  meticulously, source-by-source and cannot change all sources using this
  connection at once.

## Open questions

n/a
