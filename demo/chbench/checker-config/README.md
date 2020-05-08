Checker configuration
====================

This directory is mounted in the `/etc/checker` directory in the checker container, you can
copy [`../../../src/checker/config.toml`](../../../src/checker/config.toml) into this
directory, modify it, and run `dc.sh run checker -c /etc/checker/YOUR_FILE` and refer to
items in it.
