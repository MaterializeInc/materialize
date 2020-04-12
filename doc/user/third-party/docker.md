---
title: "Using Docker"
description: "Get details about using Materialize with Docker"
menu:
  main:
    parent: 'third-party'
---

Many of our demos rely on [Docker] and [Docker Compose] to make it easy to deploy
Materialize, along with any other infrastructure the demo needs.

For the best experience using Docker, we recommend following the guidelines
outlined here.

### Increase Docker Memory

Because many of our Docker-based demos leverage a large number of pieces of
infrastructure, we recommend running Docker with at least **8 GB** of memory.

On macOS:

1. Open **Docker for Mac**'s **Preferences** window.
1. Click **Resources**.
1. Click **Advanced**.
1. Move the **Memory** slider to at least **8.00 GB**.
1. Click **Apply & Restart**.

Note that on Linux, Docker automatically shares memory with the host machines; as long as your host machine has more than 8 GB of memory, you shouldn't run into issues.

### Use the `mzcompose` wrapper script

Materialize's Docker Compose configurations do not work directly with the
`docker-compose` command, but require a thin wrapper called `mzcompose`.

You will need [**Python 3.5+**][python] to run
`mzcompose`. This is installed by default on recent versions of macOS and most
Linux distributions.

The `mzcompose` wrapper accepts all the same arguments and options as
`docker-compose`. Use it just as you would `docker-compose`:

```shell
$ cd demo/http_logs
$ ./mzcompose up -d  # replaces `docker-compose up -d`
```

[Docker]: https://docs.docker.com/get-started/overview/
[Docker Compose]: https://docs.docker.com/compose/
[python]: https://www.python.org/downloads/
