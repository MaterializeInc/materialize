#!/usr/bin/env python3

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

log = logging.getLogger('wikipedia_live.main')


class View:

    def __init__(self):
        self.current_rows = []
        self.current_timestamp = []
        self.listeners = set([])

    def add_listener(self, conn):
        """Insert this connection into the list that will be notified on new messages."""
        # Write the latest view state to catch this listener up to the current state of the world
        conn.write_message({'deleted': [],
                            'inserted': self.current_rows,
                            'timestamp': self.current_timestamp})
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
        # Keep any rows that have not been deleted
        self.current_rows = [r for r in self.current_rows if r not in deleted]
        # And add any rows that have been inserted
        self.current_rows.extend(inserted)

        # If we have listeners configured, broadcast this diff
        if self.listeners:
            payload = {'deleted': deleted,
                       'inserted': inserted,
                       'timestamp': timestamp}

            self.broadcast(payload)


class IndexHandler(tornado.web.RequestHandler):

    async def get(self):
        async with await self.application.mzql_connection() as conn:
            async with await conn.cursor() as cursor:
                query = 'SELECT * FROM counter'
                async with await cursor.execute(query) as counts:
                    assert counts.rowcount == 1
                    row = await counts.fetchone()
                    edit_count = row[0]

        self.render('index.html', edit_count=edit_count)


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

        configured_views = ['counter', 'top_users', 'top_servers', 'top_pages']
        self.views = {view: View() for view in configured_views}

        for view in configured_views:
            ioloop.spawn_callback(self.tail_view, view)

    def mzql_connection(self):
        """Return a psycopg3.AsyncConnection object to our Materialize database."""
        return psycopg3.AsyncConnection.connect(self.dsn)

    async def tail_view(self, view_name):
        """Spawn a coroutine to tail the view and update listeners on changes."""
        log.info('Spawning coroutine to TAIL VIEW "%s"', view_name)
        async with await self.mzql_connection() as conn:
            async with await conn.cursor() as cursor:
                query = f'COPY (TAIL {view_name} WITH (PROGRESS)) TO STDOUT'
                async with cursor.copy(query) as tail:
                    inserted = []
                    deleted = []
                    async for row in tail:
                        row = row.decode('utf-8')
                        (timestamp, progressed, diff, *columns) = row.strip().split("\t")

                        if progressed == 't':
                            assert diff == '\\N'
                            self.views[view_name].update(deleted, inserted, timestamp)
                            inserted = []
                            deleted = []
                        elif diff == '-1':
                            deleted.append(columns)
                        elif diff == '1':
                            inserted.append(columns)
                        else:
                            log.error('Failed to correctly decode row %s', row.strip())

def configure_logging():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def run():
    configure_logging()

    handlers = [
        tornado.web.url(r'/', IndexHandler, name='index'),
        tornado.web.url(r'/api/v1/stream/(.*)', StreamHandler, name='api/stream'),
    ]

    base_dir = os.path.dirname(__file__)
    static_path = os.path.join(base_dir, 'static')
    template_path = os.path.join(base_dir, 'templates')

    ioloop = tornado.ioloop.IOLoop.current()
    app = Application(handlers,
                      static_path=static_path,
                      template_path=template_path,
                      ioloop=ioloop,
                      debug=True,
                      dsn='postgresql://localhost:6875/materialize')

    port = 8875
    log.info('Port %d ready to rumble!', port)
    app.listen(port)
    ioloop.start()


if __name__ == '__main__':
    run()
