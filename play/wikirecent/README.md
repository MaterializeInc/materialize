# Real Time View of Top10 Wikipedia Editors using Python and Javascript

This directory contains a demonstration application showing how to build an event-driven data
pipeline. The example application comes from our [Getting Started Guide][], where Materialize is
configured with three `MATERIALIZED VIEWS` that update as new edits are appended to the
`wikirecent` source. The three views are:

- `counter`: The count all edits that have ever been observed.
- `useredits`: The count of all edits observed, grouped by user.
- `top10`: The top-10 users, where top means most edits by count. This view is a `MATERIALIZED
  VIEW` on top of `useredits`.

[Getting Started Guide]: https://materialize.com/docs/get-started/

The Python web application then serves a basic page that renders both the current value for
`counter` and a visualization of the `top10` table. These counts and visualization are updated
as the underlying `MATERIALIZED VIEWS` are updated, using the [TAIL][] command.

[TAIL]: https://materialize.com/docs/sql/tail/

## Running it Yourself

Assuming you have Docker and Docker Compose installed, you can run this demo using the helper `up`
script:

    ./play/wikirecent/bin/up

To view the web app, run the `open-web` helper script:

    ./play/wikirecent/bin/open-web

When you're ready to stop the demo, you can run the down script to stop the services and remove
volumes:

    ./play/wikirecent/bin/down

## How This Works

This application is written as an event-driven application. This means that no components are
configured to repeatedly query or poll other applications. Instead, each application is
subscribing to updates from their dependencies. Updates are minimal in size and are expressed
soley as transformations from previous state. This has two major benefits:

- Real-time push notifications. Applications are notified as soon as data changes and are not
  required to repeatedly poll for the same information over and over again.

- Economy of resources required. Each result set only contains information about datum that have
  changed. Fewer bytes are sent over the network and processing updates is more efficient.

### Services and Service Dependency

There are 4 major components that comprise this stack:

- `stream`: A service to curl [Wikimedia's Recent Change Stream][] and write append the
  results to a file called `recentchanges`.
- `materialized`: An instance of `materialized` configured to `TAIL` the `recentchanges` file and
  maintain the `counter`, `useredits` and `top10` `MATERIALIZED VIEWS`.
- `app`: A Python web server, written as an asynchronous application using [tornado-web][] and
  [psycopg3][], to `TAIL` the `counter` and `top10` views and present updates to these views over
  websockets.
- The user's web browser. The webpage rendered by `app` includes Javascript code that opens two
  websockets to `app`. Each message, pushed by `app`, contains an update that is then rendered by
  the user's web browser.

[Wikimedia's Recent Change Stream]: https://stream.wikimedia.org/v2/stream/recentchange
[tornado-web]: https://www.tornadoweb.org/en/stable/
[psycopg3]: https://www.psycopg.org/psycopg3/

Thus, the flow of data looks like this:

    Wikimedia --> stream --> recentchanges --> materialized --> app --> browser

Contrast this with a "typical web app" backed by a SQL database, where each application is making
repeated requests to upstream systems:

    Wikimedia <-- stream --> database <-- app <-- browser

## Extras

### Open a psql client to Materialize

The Materialize database is not exposed on a well-numbered port and instead docker-compose chooses
a random high-numbered port. To open a Postgres REPL, connected to the Materialized instance
running as part of this demo, run:

    ./play/wikirecent/bin/open-psql

### Streaming top10 from Materialize to your console

If you have [psycopg3][] installed on your local system ([psycopg3 install instructions][]), you
can see the same data being pushed to the Python server by running:

    ./play/wikirecent/bin/tail_top10

The output from this script corresponds to the row-oriented results returned by materialized to
the web server. The implementation of this script in
[extras/async_tail.py](./extras/async_tail.py).

[psycopg3 install instructions]: https://www.psycopg.org/psycopg3/docs/install.html

### TODO: Streaming top10 from the Python Web Server to your console

If you have the [websockets][] module installed on your local system, you can stream the same set
of updates directly from the web server:

    ./play/wikirecent/bin/stream_top10

The output from this script corresponds to the batch oriented, JSON results returned by the web
server to the Javascript client. The implementation of this script in
[extras/async_stream.py](./extras/async_stream.py).

[websockets]: https://websockets.readthedocs.io/en/stable/intro.html
