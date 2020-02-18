Peeker configuration
====================

This directory is mounted in the `/etc/peeker` directory in the peeker container, you can
copy [`../../../src/peeker/config.toml`](../../../src/peeker/config.toml) into this
directory, modify it, and run `dc.sh run peeker -c /etc/peeker/YOUR_FILE` and refer to
items in it.
