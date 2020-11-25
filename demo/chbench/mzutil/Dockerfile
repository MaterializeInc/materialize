FROM python:3.9-slim

RUN pip install psycopg2-binary

# This is the directory that we expect to be mounted by Docker so that we can
# save files across Docker runs
RUN mkdir -p /snapshot

COPY await_view_states /usr/local/bin/await_view_states
RUN chmod +x /usr/local/bin/await_view_states

COPY snapshot_view_states /usr/local/bin/snapshot_view_states
RUN chmod +x /usr/local/bin/snapshot_view_states
