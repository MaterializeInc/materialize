#!/usr/bin/env python3

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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
    def __init__(self):
        self.current_rows = []
        self.current_timestamp = []
        self.listeners = set([])

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

    def remove_listener(self, conn):
        """Remove this connection from the list that will be notified on new messages."""
        try:
            self.listeners.remove(conn)
        except KeyError:
            pass

    def update(self, deleted, inserted, timestamp):
        """Update our internal view based on this diff."""
        self.current_timestamp = timestamp

        # Remove any rows that have been deleted
        for r in deleted:
            self.current_rows.remove(r)

        # And add any rows that have been inserted
        self.current_rows.extend(inserted)

        # If we have listeners configured, broadcast this diff
        if self.listeners:
            payload = {"deleted": deleted, "inserted": inserted, "timestamp": timestamp}

            self.broadcast(payload)


class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("index.html")


class StreamHandler(tornado.websocket.WebSocketHandler):
    def open(self, view):
        self.view = view
        self.application.views[view].add_listener(self)

    def on_close(self):
        self.application.views[self.view].remove_listener(self)


class Application(tornado.web.Application):
    def __init__(self, *args, dsn, ioloop, **kwargs):
        super().__init__(*args, **kwargs)
        self.dsn = dsn

        configured_views = ["counter", "top10"]
        self.views = {view: View() for view in configured_views}

        for view in configured_views:
            ioloop.add_callback(self.tail_view, view)

    def mzql_connection(self):
        """Return a psycopg3.AsyncConnection object to our Materialize database."""
        return psycopg3.AsyncConnection.connect(self.dsn)

    async def tail_view(self, view_name):
        """Spawn a coroutine that sets up a coroutine to process changes from TAIL."""
        log.info('Spawning coroutine to TAIL VIEW "%s"', view_name)
        async with await self.mzql_connection() as conn:
            async with await conn.cursor() as cursor:
                query = f"COPY (TAIL {view_name} WITH (PROGRESS)) TO STDOUT"
                async with cursor.copy(query) as tail:
                    await self.tail_view_inner(view_name, tail)

    async def tail_view_inner(self, view_name, tail):
        """Read rows from TAIL, converting them to updates and broadcasting them."""
        inserted = []
        deleted = []
        async for row in tail:
            row = row.decode("utf-8")
            (timestamp, progressed, diff, *columns) = row.strip().split("\t")

            # This row serves as a synchronization primitive indicating that all
            # rows for an update have been read. We should publish this update.
            if progressed == "t" and diff == "\\N":
                self.views[view_name].update(deleted, inserted, timestamp)
                inserted = []
                deleted = []
            # This is a row that we should insert or delete "diff" number of times
            # Simplify our implementation by creating "diff" copies of each row instead
            # of tracking counts per row
            else:
                try:
                    diff = int(diff)
                except ValueError:
                    raise

                if diff < 0:
                    deleted.extend([columns for _ in range(diff, 0)])
                elif diff > 0:
                    inserted.extend([columns for _ in range(0, diff)])
                else:
                    raise ValueError(f"Bad data from TAIL: {row.strip()}")


def configure_logging():
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def run():
    configure_logging()

    handlers = [
        tornado.web.url(r"/", IndexHandler, name="index"),
        tornado.web.url(r"/api/v1/stream/(.*)", StreamHandler, name="api/stream"),
    ]

    base_dir = os.path.dirname(__file__)
    static_path = os.path.join(base_dir, "static")
    template_path = os.path.join(base_dir, "templates")

    ioloop = tornado.ioloop.IOLoop.current()
    app = Application(
        handlers,
        static_path=static_path,
        template_path=template_path,
        ioloop=ioloop,
        debug=True,
        dsn="postgresql://materialized:6875/materialize",
    )

    port = 8875
    log.info("Port %d ready to rumble!", port)
    app.listen(port)
    ioloop.start()


if __name__ == "__main__":
    run()
