---
title: "Database object explorer"
description: "Explore the objects in your databases from the Materialize console."
disable_toc: true
menu:
  main:
    parent: console
    weight: 15
    identifier: console-data
---

Under **Data**, the Materialize Console
provides a database object explorer.

![Image of the Materialize Console Database Object
Explorer](/images/console/console-data-explorer.png "Materialize Console Database Object Explorer")

<span class="caption">
When you select <em>Data</em>, the left panel collapses to reveal the database
object explorer.
</span>

You can inspect the objects in your databases by navigating to the object.

|Object|Available information|
|---|---|
|Connections|<li>Details: The [`CREATE CONNECTION`](/sql/create-connection/) SQL statement.</li>|
|Indexes|<li>Details: The [`CREATE INDEX`](/sql/create-index/) SQL statement.</li><li>Workflow: Details about the index (e.g., status), freshness, upstream and downstream objects. </li><li>Visualize: Dataflow visualization.</li>|
|Materialized Views|<li>Details: The [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/) SQL statement.</li><li>Workflow: Details about the materialized view (e.g., status), freshness, upstream and downstream objects.</li><li>Visualize: Dataflow visualization.</li>|
|Sinks|<li>Overview: View the sink metrics (e.g., messages/bytes produced) and details (e.g., Kafka topic).</li><li>Details: The [`CREATE SINK`](/sql/create-sink/) SQL statement.</li><li>Errors: Errors associated with the sink.</li><li>Workflow: Details about the sink (e.g., status), freshness, upstream and downstream objects.</li>|
|Sources|<li>Overview: View the ingestion metrics (e.g., Ingestion lag, messages/bytes received, Ingestion rate), Memory/CPU/Disk usage</li><li>Details: The [`CREATE SOURCE`](/sql/create-source/) SQL statement.</li><li>Errors: Errors associated with the source.</li><li>Subsources: List of associated subsources and their status.</li><li>Workflow: Details about the source (e.g.,status), freshness, upstream and downstream objects.</li><li>Indexes: Indexes on the source.</li>|
|Subsources|<li>Details: The `CREATE SUBSOURCE` SQL statement.</li><li>Columns: Column details.</li><li>Workflow: Details about the subsource (e.g.,status), freshness, upstream and downstream objects.</li><li>Indexes: Indexes on the subsource.</li>|
|Tables|<li>Details: The [`CREATE TABLE`](/sql/create-table/) SQL statement.</li><li>Workflow: Details about the table (e.g., status), freshness, upstream and downstream objects.</li><li>Columns: Column details.</li><li>Indexes: Indexes on the table.</li>|
|Views|<li>Details: The [`CREATE VIEW`](/sql/create-view/) SQL statement.</li><li>Columns: Column details.</li><li>Indexes: Indexes on the view. </li>|

#### Sample source overview

![Image of the Source Overview for auction_house
index](/images/console/console-data-explorer-source-overview.png "Source Overview for auction_house")

#### Sample index workflow

![Image of the Index Workflow for wins_by_item
index](/images/console/console-data-explorer-index-workflow.png "Index Workflow for wins_by_item index")
