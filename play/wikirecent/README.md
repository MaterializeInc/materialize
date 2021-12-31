# Real-time view of top 10 Wikipedia editors using Python and JavaScript

This directory contains a demonstration application showing how to build an
event-driven data pipeline. The example application comes from our [Getting
Started Guide], where Materialize is configured with three materialized views
that update as new edits are appended to the `wikirecent` source. The three
views are:

- `counter`: The count all edits that have ever been observed.
- `useredits`: The count of all edits observed, grouped by user.
- `top10`: The top-10 users, where top means most edits by count. This view is a materialized
  view on top of `useredits`.

The Python web application then serves a basic page that renders both the
current value for `counter` and a visualization of the `top10` table. These
counts and visualization are updated as the underlying materialized views are
updated, using the [`TAIL`] command.

## Running it yourself

To run this demo using, run `mzcompose up` within the `play/wikirecent`
directory:

```
git clone https://github.com/MaterializeInc/materialize
cd play/wikirecent
./mzcompose run demo
```

To view the web app in your local browser, run the `mzcompose web` command:

```
./mzcompose web server
```

Once you're done, run `mzcompose down` to stop services and delete the
associated volumes:

```
./mzcompose down -v
```

## How this works

This application is written as an event-driven application. This means that no
components are configured to repeatedly query or poll other applications.
Instead, each application is subscribing to updates from their dependencies.
Updates are minimal in size and are expressed soley as transformations from
previous state. This has two major benefits:

- Real-time push notifications. Applications are notified as soon as data
  changes and are not required to repeatedly poll for the same information over
  and over again.

- Economy of resources required. Each result set only contains information about
  rows that have changed. Fewer bytes are sent over the network and processing
  updates is more efficient.

### Services and service dependencies

There are 4 major components that comprise this stack:

- `stream`: A service to curl [Wikimedia's recent change stream] and append the
  results to a file called `recentchanges`.
- `materialized`: An instance of `materialized` configured to `TAIL` the
  `recentchanges` file and maintain the `counter`, `useredits` and `top10`
  materialized views.
- `app`: A Python web server, written as an asynchronous application using
  [tornado-web] and [psycopg3], to tail the `counter` and `top10` views and
  present updates to these views over websockets.
- The user's web browser. The webpage rendered by `app` includes JavaScript code
  that opens two websockets to `app`. Each message, pushed by `app`, contains an
  update that is then rendered by the user's web browser.

Thus, the flow of data looks like this:

    Wikimedia --> stream --> recentchanges --> materialized --> app --> browser

Contrast this with a "typical web app" backed by a SQL database, where each application is making
repeated requests to upstream systems:

    Wikimedia <-- stream --> database <-- app <-- browser

## Debugging / utilities

### Open a psql client to `materialized`

The `materialized` instance is not exposed on a well-known port and instead
mzcompose chooses a random high-numbered port. So to open a Postgres shell
connected to the `materialized` instance running as part of this demo, run:

```
$ psql -h localhost -p $(./mzcompose list-ports materialized) -U materialize materialize
```

To then stream the `top10` view to your console:

```
materialize=> COPY (TAIL top10) TO STDOUT;
```

### Streamping `top10` from the Python web server to your console

You can use curl to stream the contents of `top10` via the Python web server,
rather than from Materialize directly:

```
curl --no-buffer --output - \
  -H "Sec-WebSocket-Key: +" -H "Sec-WebSocket-Version: 13" -H "Connection: Upgrade" -H "Upgrade: websocket" \
  "http://localhost:$(./mzcompose list-ports server)/api/stream/top10"
```

The output corresponds to the JSON results returned by the web server to the
JavaScript client.

[Getting Started Guide]: https://materialize.com/docs/get-started/
[TAIL]: https://materialize.com/docs/sql/tail/
[wikimedia's recent change stream]: https://stream.wikimedia.org/v2/stream/recentchange
[tornado-web]: https://www.tornadoweb.org/en/stable/
[psycopg3]: https://www.psycopg.org/psycopg3/
[asyncpg]: https://github.com/MagicStack/asyncpg
