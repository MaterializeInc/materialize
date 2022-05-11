---
title: "Run SQL on Streaming Logs"
description: "Find out how Materialize can extract meaningful data from logs in real time."
aliases:
  - /demos/log-parsing/
menu:
    main:
        parent: quickstarts
weight: 30
---

{{% demos-warning %}}

**tl;dr** Materialize can extract meaningful data from logs in real time.

Servers, especially busy ones, can emit a vast amount of logging data. Given
that logs are unstructured strings, it's challenging to draw inferences from
them. This inaccessibility often means that even though logs contain a lot of
potential, teams don't capitalize on them.

Materialize, though, offers the chance to extract and query data from your logs.
An instance of `materialized` can continually read a log file, impose a
structure on it, and then let you define views you want to maintain on that
data&mdash;just as you would with any SQL table. This opens up new opportunities
for both real-time business and operational analysis.

## Overview

In this demo, we'll look at parsing server logs for a mock e-commerce site, and
extracting some business insights from them.

In the rest of this section, we'll cover the "what" and "why" of our proposed
deployment using Materialize to provide real-time log parsing.

### Server

Our primary source of data is the e-commerce site's web server, which lets users
search for and browse the company's product pages.

A web server is a great place to aggregate logs because it represents users'
primary point of contact with a company. This gives us many different dimensions
of data among most, if not all, of our users.

In this example, we'll be running one server, which pipes all of its output to a
log file.

### Load generator

This demo's load generator simulates user traffic to the web browser&mdash;users
can either:

- View the home page
- Search for products
- View product detail pages

The load generator simulates ~300 users/second at its highest, with ~10% of the
users attritioning off the site.

### Materialize

Materialize presents an interface to ingest, parse, and query the server's log
files.

In this demo, Materialize...

- Creates a dynamic file source for the log file. This means it continually
  watches the file for updates (known as "tailing the file"), and streams new
  data to materialized views that depend on the source. Because of Materialize's
  architecture, this means that new events are fully processed by views with
  incredibly low latency.
- Imposes a structure on the log files passing incoming lines through a regular
  expression, using named capture groups to create columns
- Provides a SQL interface to query the structured version of the log files.

We will connect to Materialize through `psql`.

### Diagram

{{<
    figure src="/images/demos/log_parsing_architecture_diagram.png"
    alt="Load generator <-HTTP-> Web server -> logs -> materialized <-SQL-> psql"
    width="300"
>}}

## Conceptual overview

Our overall goal in this demo is to take unstructured log files, impose
structure on them through regex, and then perform queries to extract some
analytical understanding of the logs' data.

In this section, we'll cover the conceptual approach to this problem in
Materialize, which includes:

1. Understanding your logs' implicit structure.
1. Imposing a structure on your logs using regex.
1. Creating sources from your logs.
1. Querying sources to extract insights from your logs.

In the next section [Run the demo](#run-the-demo), we'll have a chance to see
some of these things in action.

### Understand the logs' structure

Log files are often just strings of text delimited by newlines&mdash;this makes
them difficult to use in a relational model. However, given that they're
formatted consistently, it's possible to impose structure on the logs with
regular expressions--which is exactly what we'll do.

First, it's important to understand what structure our logs have. Below is an
example of a few lines from our web server's log file:

```nofmt
248.87.122.109 - - [28/Jan/2020 17:08:19] "GET /search/?kw=K8oN HTTP/1.1" 200 -
203.153.87.134 - - [28/Jan/2020 17:08:19] "GET /search HTTP/1.1" 308 -
12.234.172.170 - - [28/Jan/2020 17:08:20] "GET /detail/HGwL HTTP/1.1" 200 -
```

We can see that some fields we might be interested in include:

Field | Example
------|--------
IP addresses | `248.87.122.109`
Timestamps | `28/Jan/2020 17:08:19`
Full page paths | `/search/?kw=K8oN`
Search terms | `K8oN` in `GET /search/?kw=K8oN`
Viewed product pages | `HGwL` in `GET /detail/HGwL`
HTTP status code | `200`

### Impose a structure with regex

Once we understand our logs' structure, we can formalize it with a regular
expression. In this example, we'll use named capture groups to generate columns
(e.g. `(?P<ip>...)` creates a capture group named `ip`).

While you don't necessarily need to understand the following regex, here's an
example of how we can structure the above logs:

```regex
(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(?P<ts>[^]]+)\] "(?P<path>(?:GET /search/\?kw=(?P<search_kw>[^ ]*) HTTP/\d\.\d)|(?:GET /detail/(?P<product_detail_id>[a-zA-Z0-9]+) HTTP/\d\.\d)|(?:[^"]+))" (?P<code>\d{3}) -
```

In this regex, we've created the following columns:

Column | Expresses
-------|----------
`ip` | Users' IP address
`ts` | Events' timestamp
`path` | Paths where the event occurred
`search_kw` | Keywords a user searched for
`product_detail_id` | IDs used to differentiate each product
`code` | HTTP codes

In Materialize, if a capture group isn't filled by the input string, the row
simply has a _NULL_ value in the attendant column.

### Create sources from logs

With our regex and logs in hand, we can create sources from our log files and
impose a structure on them:

```sql
CREATE SOURCE requests
FROM FILE '/log/requests' WITH (tail = true)
FORMAT REGEX '(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(?P<ts>[^]]+)\] "(?P<path>(?:GET /search/\?kw=(?P<search_kw>[^ ]*) HTTP/\d\.\d)|(?:GET /detail/(?P<product_detail_id>[a-zA-Z0-9]+) HTTP/\d\.\d)|(?:[^"]+))" (?P<code>\d{3}) -';
```

While you can find more details about this statement in [`CREATE
SOURCE`](/sql/create-source/), here's an explanation of the
arguments:

Argument | Function
---------|---------
`requests` | The source's name
`/log/requests` | The location of the file
`FORMAT REGEX '...'` | The regex that structures our logs and generates column names, which we've outlined in [Impose structure with regex](#impose-a-structure-with-regex).
`tail = true` | Indicates to Materialize that this file is dynamically updated and should be watched for new data.

In essence, what we've said here is that we want to continually read from the
log file, and take each unseen string in it, and extract the columns we've
specified in our regex.

After creating the source, we can validate that it's structured like we expect
with `SHOW COLUMNS`.

```sql
SHOW COLUMNS FROM requests;
```
```nofmt
       name        | nullable |  type
-------------------+----------+--------
 ip                | t        | text
 ts                | t        | text
 path              | t        | text
 search_kw         | t        | text
 product_detail_id | t        | text
 code              | t        | text
 mz_line_no        | f        | bigint
(7 rows)
```

This looks like we expect, so we're good to move on.

### Query the logs' source

After creating a source, we can create materialized views that depend on it to
query the now-structured logs.

Looking at this structure, we can extract some inferences. For example, if we
assume that each user arrives at our site from a unique IP address, getting a
count of unique IP addresses can provide a count of users.

```sql
SELECT count(DISTINCT ip) FROM requests;
```

And then we can create a materialized view that embeds this query:

```sql
CREATE MATERIALIZED VIEW unique_visitors AS
    SELECT count(DISTINCT ip) FROM requests;
```

From here, we can check the results of this view:

```sql
SELECT * FROM unique_visitors;
```

In a real environment, which we'll see in just a second, these results get
returned to us very quickly because Materialize stores the result set in memory.

## Run the demo

Our demo has a setup script that spins up a fully working instance of
Materialize that already reads and structures our log files (you can see the
steps we take in `demo/http_logs/cli/setup.sh`). So in this section, we'll walk
through spinning up the demo, and making sure that we see the things we expect.
In a future iteration, we'll make this demo more interactive.

### Preparing the environment

1. [Set up Docker and Docker compose](/integrations/docker), if you haven't
   already.

1. Verify that you have Python 3 or greater installed.

    ```shell
    $ python3 --version
    Python 3.7.5
    ```

1. Clone the Materialize repo at the latest release:

    ```shell
    git clone https://github.com/MaterializeInc/demos.git
    ```

1. Move to the `demos/http-logs` directory:

    ```shell
    cd demos/http-logs
    ```

    You can also find the demo's code on
    [GitHub](https://github.com/MaterializeInc/materialize/tree/{{< version >}}/demo/http_logs).

1. Download and start all of the components we've listed above by running:

   ```shell
   docker-compose up -d
   ```

   Note that downloading the Docker images necessary for the demo can take quite
   a bit of time (upwards of 3 minutes, even on very fast connections).

### Understanding sources & views

Now that our deployment is running (and looks like the diagram shown above), we
can see that Materialize is ingesting the logs and structuring them. We'll also
get a chance to see how Materialize can handle queries on our data.

1. Launch a new terminal window and `cd materialize/demo/http_logs`.

1. Launch `psql` by running:

    ```shell
    docker-compose run cli
    ```

1. Within `psql`, ensure you have all of the necessary sources, which represent
   all of the tables from MySQL:

    ```sql
    SHOW SOURCES;
    ```
    ```nofmt
       name
    ----------
     requests
    (1 row)
    ```

    This source was created using the `CREATE SOURCE` statement we wrote
    [here](#create-sources-from-logs).

1. We can look at the structure of the `requests` source with `SHOW COLUMNS`:

    ```sql
    SHOW COLUMNS FROM requests;
    ```
    ```nofmt
           name        | nullable |  type
    -------------------+----------+--------
     ip                | t        | text
     ts                | t        | text
     path              | t        | text
     search_kw         | t        | text
     product_detail_id | t        | text
     code              | t        | text
     mz_line_no        | f        | bigint
    (7 rows)
    ```

    As you'll remember, this is the structure [we expected when creating a
    source from the logs](#create-sources-from-logs).

1. From here, we can create arbitrary queries from this structure. We've created
   a few views that represent some queries you might want to perform with this
   data.

    See the views we've created with `SHOW VIEWS`:

    ```sql
    SHOW VIEWS;
    ```
    ```nofmt
             name
    ----------------------
     avg_dps_for_searcher
     top_products
     unique_visitors
    (3 rows)
    ```

    View | Description
    -----|------------
    `avg_dps_for_searcher` | Average number of detail pages viewed by users who search
    `top_products` | Most commonly viewed product pages
    `unique_visitors` | Count of unique visitors, determined by IP address

1. To see the query that underlies a view, use `SHOW CREATE VIEW`:

    ```sql
    SHOW CREATE VIEW unique_visitors;
    ```

    From these results, we can see that the query that this view describes is:

    ```sql
    CREATE MATERIALIZED VIEW unique_visitors AS
        SELECT count(DISTINCT ip) FROM requests;
    ```

1. To view the results of this query, run:

    ```sql
    SELECT * FROM unique_visitors;
    ```

    You'll note that the result should come back pretty quickly.

    Run this query a few times to show that it continues to increase as the load
    generator uses more different IP addresses.

    Now that you've seen how this is done, feel free to explore the other views
    or use [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view) to
    explore Materialize's capabilities yourself.

## Recap

In this demo, we saw:

- How to create a source from dynamic file
- How Materialize can structure log files
- How to define sources and views within Materialize
- How to query views to extract data from your logs

## Related pages

- [Microservice demo](/quickstarts/microservice/)
- [`CREATE SOURCE`](/sql/create-source)
- [Functions + Operators](/sql/functions)
