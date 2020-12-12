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
as the underlying `MATERIALIZED VIEWS` are updated.

## Running it Yourself

Assuming you have Docker and Docker Compose installed, you can run this demo using the helper `up`
script:

    ./play/wikirecent/bin/up

To view the web app, run the `open-web` helper script (this step is required to expose the web
app's HTTP port):

    ./play/wikirecent/bin/open-web

When you're ready to stop the demo, you can run the down script to stop the services and remove
volumes:

    ./play/wikirecent/bin/down

## How This Works

This application is written as an event-driven application. This means that no components are
configured to repeatedly query or poll other applications. Instead, each application is
subscribing to updates from their dependencies. Updates are minimal in size and are expressed
soley as transformations from previous state. This has two major benefits:

- Real-time push nofications. Applications are notified as soon as data changes and are not
  required to repeatedly poll for the same information over and over again.

- Economy of resources required. Each result set only contains information about datum that have
  changed. Fewer bytes are sent over the network and processing updates is more efficient.

### Services and Service Dependency

There are 4 major components that should be discussed when talking about this stack:

- `stream`: A service to curl [Wikimedia's Recent Change Stream][] and write append the
  results to a file called `recentchanges`.
- `materialized`: An instance of `materialized` configured to `TAIL` the `recentchanges` file and
  maintain the `counter`, `useredits` and `top10` `MATERIALIZED VIEWS`.
- `app`: A Python web server, written as an asynchronous application using [tornado-web][] and
  [psycopg3][], to `TAIL` the `counter` and `top10` views and present the updates to these views
  over websockets.
- The user's web browser. The webpage rendered by `app` includes Javascript code that opens two
  websockets to `app`. Each message, pushed by `app`, contains an update that is then rendered by
  the user's web browser.

[Wikimedia's Recent Change Stream]: https://stream.wikimedia.org/v2/stream/recentchange
[tornado-web]: https://www.tornadoweb.org/en/stable/
[psycopg3]: https://www.psycopg.org/psycopg3/

Thus, the flow of data looks like this:

    Wikimedia --push-> stream --append-> recentchanges --tail-> materialized --update-> app --update-> browser

Contrast this with a "typical web app" backed by a SQL database:

    Wikimedia <-poll-- stream --update-> database <-query-- app <-poll-- browser
