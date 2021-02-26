#!/usr/bin/env python3

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Demonstration Tornado Web Server for Materialize Wikirecent Use Case.

This file is written in a bottom-up structure. The code flows as follows:

- Call `run` to create a Tornado application with HTTP and websocket handlers, start background
  TAIL VIEW coroutines and start the event loop.
- `IndexHandler` is responsible for serving our HTML / Javacript page.
- `StreamHandler` is responsible for accepting new client connections, adding them as listeners on
  `View` objects and removing them when connections are closed.
- `Application` is responsible for creating an internal `View` object per materialized view that
  we are interesting in tailing. It's also the class that enables `StreamHandler` objects to
  locate `View` objects.
- `View` objects represents a mirror copy of a materialized view in materialized. Views spawn a
  background coroutine that runs the `TAIL` command, processing rows and turning them into updates
  that can be broadcast to listeners. The background thread is also responsible for maintaining an
  internal copy of the view that can be used to initialize state for new listeners.
"""

import argparse
import collections
import logging
import os
import pprint
import sys

import psycopg3
import tornado.ioloop
import tornado.platform.asyncio
import tornado.web
import tornado.websocket

log = logging.getLogger("wikipedia_live.main")


class View:
    def __init__(self, dsn, view_name):
        """Create a View object.

        :Params:
            - `dsn`: The postgres connection string to locate materialized.
            - `view_name`: The name of the materialized view to TAIL.
        """
        self.current_rows = []
        self.current_timestamp = None
        self.dsn = dsn
        self.listeners = set([])
        self.view_name = view_name

    def add_listener(self, conn):
        """Insert this connection into the list that will be notified on new messages."""
        # Write the latest view state to catch this listener up to the current state of the world
        conn.write_message(
            {
                "deleted": [],
                "inserted": self.current_rows,
                "timestamp": self.current_timestamp,
            }
        )
        self.listeners.add(conn)

    def broadcast(self, payload):
        """Write the message to all listeners. May remove closed connections."""

        closed_listeners = set()
        for listener in self.listeners:
            try:
                # Each broadcast is a dictionary with two keys: 'inserted' and 'deletes'
                # The values are lists of rows, where each row is a list of column values
                listener.write_message(payload)
            except tornado.websocket.WebSocketClosedError:
                closed_listeners.add(listener)

        for closed_listener in closed_listeners:
            self.listeners.remove(closed_listener)

    def mzql_connection(self):
        """Return a psycopg3.AsyncConnection object to our Materialize database."""
        return psycopg3.AsyncConnection.connect(self.dsn)

    def remove_listener(self, conn):
        """Remove this connection from the list that will be notified on new messages."""
        try:
            self.listeners.remove(conn)
        except KeyError:
            pass

    async def tail_view(self):
        """Spawn a coroutine that sets up a coroutine to process changes from TAIL.

        Delegates handling of the actual query / rows to `tail_view_inner`.
        """
        log.info('Spawning coroutine to TAIL VIEW "%s"', self.view_name)
        async with await self.mzql_connection() as conn:
            async with await conn.cursor() as cursor:
                await self.tail_view_inner(cursor)

    async def tail_view_inner(self, cursor):
        """Read rows from TAIL, converting them to updates and broadcasting to listeners.

        :Params:
            - `cursor`: A psycopg3 cursor that is configured to read rows from a TAIL query.
        """
        inserted = []
        deleted = []
        query = f"TAIL {self.view_name} WITH (PROGRESS)"
        async for (timestamp, progressed, diff, *columns) in cursor.stream(query):
            # The progressed column serves as a synchronization primitive indicating that all
            # rows for an update have been read. We should publish this update.
            if progressed:
                self.update(deleted, inserted, timestamp)
                inserted = []
                deleted = []
                continue

            # Simplify our implementation by creating "diff" copies of each row instead
            # of tracking counts per row
            if diff < 0:
                deleted.extend([columns] * abs(diff))
            elif diff > 0:
                inserted.extend([columns] * diff)
            else:
                raise ValueError(
                    f"Bad data in TAIL: {timestamp}, {progressed}, {diff}, {columns}"
                )

    def update(self, deleted, inserted, timestamp):
        """Update our internal view based on this diff and broadcast the update to listeners.

        :Params:
            - `deleted`: The list of rows that need to be removed.
            - `inserted`: The list of rows that need to be added.
            - `timestamp`: The materialize internal timestamp for this view.
        """
        self.current_timestamp = int(timestamp)

        # TODO: Insert new rows after deleting, once #5827 is fixed
        # Add any rows that have been inserted
        self.current_rows.extend(inserted)

        # Remove any rows that have been deleted
        for r in deleted:
            self.current_rows.remove(r)

        # If we have listeners configured, broadcast this diff
        if self.listeners:
            payload = {
                "deleted": deleted,
                "inserted": inserted,
                "timestamp": self.current_timestamp,
            }
            self.broadcast(payload)


class IndexHandler(tornado.web.RequestHandler):
    """Simple Handler to render our index page.

    This is a template, as opposed to a static HTML page, because we render the reverse_url
    location for our stream Handlers as the websocket paths.
    """

    def get(self):
        self.render("index.html")


class StreamHandler(tornado.websocket.WebSocketHandler):
    """Class for handling individual websocket connections."""

    def open(self, view):
        """New websocket connection; broadcast views updates to this listener.

        :Params:
            - `view`: The name of the materialized view to follow.
        """
        self.view = view
        self.application.views[view].add_listener(self)

    def on_close(self):
        """Websocket connection closed; remove this listener."""
        self.application.views[self.view].remove_listener(self)


class Application(tornado.web.Application):
    def __init__(self, *args, configured_views, dsn, **kwargs):
        """Create an Application object.

        This object holds a reference to our View objects so that our websocket listeners can add
        / remove listeners as connections are opened / closed.

        :Params:
            - `configured_views`: A list of materialized view names.
            - `dsn`: A postgres connection string for our materialized instance.
        """
        super().__init__(*args, **kwargs)
        self.views = {view: View(dsn, view) for view in configured_views}

    def tail_views(self, ioloop):
        """Kick-off background coroutines to TAIL views and broadcast updates to Listeners."""
        for view in self.views.values():
            ioloop.add_callback(view.tail_view)


def configure_logging():
    """Setup our desired logging configuration."""
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def run(dsn, views):
    """Create the Wikirecent Tornado Application.

    Create a Tornado application configured with our HTTP / Websocket handlers and start listening
    on the configured port.
    """
    configure_logging()

    handlers = [
        tornado.web.url(r"/", IndexHandler, name="index"),
        tornado.web.url(r"/api/v1/stream/(.*)", StreamHandler, name="api/stream"),
    ]

    base_dir = os.path.dirname(__file__)
    static_path = os.path.join(base_dir, "static")
    template_path = os.path.join(base_dir, "templates")

    app = Application(
        handlers,
        static_path=static_path,
        template_path=template_path,
        debug=True,
        configured_views=views,
        dsn=dsn,
    )

    app.listen(args.listen_port, args.listen_addr)
    log.info("Address %s:%d ready to rumble!", args.listen_addr, args.listen_port)

    ioloop = tornado.ioloop.IOLoop.current()
    app.tail_views(ioloop)

    # Serve until program completion
    ioloop.start()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--dbhost", help="materialized hostname", default="materialized", type=str
    )
    parser.add_argument(
        "--dbname", help="materialized database name", default="materialize", type=str
    )
    parser.add_argument(
        "--dbport", help="materialized port number", default=6875, type=int
    )
    parser.add_argument(
        "--dbuser", help="materialized username", default="materialize", type=str
    )

    parser.add_argument(
        "--listen-port", help="Network port to bind to", default="8875", type=int
    )

    parser.add_argument(
        "--listen-addr", help="Network address to bind to", default="0.0.0.0", type=str
    )

    parser.add_argument(
        "views", type=str, nargs="+", help="Views to expose as websockets"
    )

    args = parser.parse_args()

    dsn = f"postgresql://{args.dbuser}@{args.dbhost}:{args.dbport}/{args.dbname}"

    run(dsn, args.views)
