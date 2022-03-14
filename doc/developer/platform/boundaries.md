# Materialize platform: networked boundaries

This document describes the characteristics of the network boundaries between the components of the Materialize
platform.
In its current form it raises more questions than it provides answers, which is as intended.

## Storage and compute

Materialize's storage layer is responsible for ingesting data from external sources, providing some form of durability,
and publishing data to external subscribers.
The compute layer is responsible for transforming data based on queries installed by the user.
Together they provide the core computational resource for Materialize.
In the following, we describe the requirements of the network boundary between the two components.

## STORAGE boundary architecture

The STORAGE hosts a server socket accepting connections from COMPUTE clients.
* A single connection hosts many multiplexed subscriptions.
* A runtime component mediates between the network and dataflow layer.
* It offers subscriptions to sources, with a per-worker granularity.

## DATAFLOW boundary architecture

The DATAFLOW hosts a client connected to a STORAGE server.
* A single connection hosts many multiplexed subscriptions.
* A runtime component mediates between the network and dataflow layer.

## Commands to STORAGE

The storage service accepts the following commands over the network:
* `Subscribe(dataflow, source, worker)`: The identified dataflow subscribes to updates from the named source worker.
* `Unsubscribe(dataflow, source, worker)`: The identified dataflow unsubscribes from updates from the named source worker.
  This causes resouces on STORAGE to be freed.

## Responses from STORAGE to COMPUTE

The storage service updates the compute service using the following responses:
* `Data(dataflow, source, worker, events)`: The captured data as `events` for the named dataflow from the identified
  source, from a specific source worker.
  We support two variants of this response to cover both the data and error collections.

## Unsolved problems

* We cannot support a multi-process Timely STORAGE cluster because we have one process-local server endpoint.
* Multiplexing many subscriptions on a single socket might cause performance problems, but sockets can support several
  gigabit per second bandwidth, so this might not be a major concern.
* We need to define what reconnects the network layer should support.
* We need to define to what level the boundary needs to support HA/restarts.
  Ideally, it is outside the scope of the boundary.
