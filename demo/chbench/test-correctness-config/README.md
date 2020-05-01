Corretness checker configuration
====================

This directory is mounted in the `/etc/test-correctness` directory in the test-correctness container, you can
copy [`../../../src/test/correctness/config.toml`](../../../test/correctness/chbench-config.toml) into this
directory, modify it, and run `./mzcompose test-correctness -c /etc/test-correctness/YOUR_FILE` and refer to
items in it.
