---
title: "Build a user notification service"
description: "  "
draft: true
menu:
    main:
        parent: quickstarts
weight: 20
---


## What are we building

Materialize is an event-driven database, making it ideal for event-driven notification use cases. To power notifications and alerts, users can:

1. Write a SQL query such that each row corresponds to a notification
1. Materialize the query in a materialized view to keep the results up-to-date.
1. Stream updates (new rows) to the view out to a new Kafka topic via a **Sink**.
1. Use a service to consume from the sink topic and trigger notifications.

[DIAGRAM]

## Prerequisites

## Example Repo

## Step one: Connect source data

### Primary Database

### Analytics and Behavioral Data

### Third Party APIs

## Step two: Join, Aggregate, Transform data with SQL

### Creating Views

### Creating Materialized Views

### Extra Credit: Moving it to dbt

## Step three: Create a SINK

### Exactly Once semantics

## Step four: Consuming from the Sink
