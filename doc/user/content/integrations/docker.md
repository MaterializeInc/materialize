---
title: "Using Docker"
description: "Get details about using Materialize with Docker"
aliases:
  - /third-party/docker/
menu:
  main:
    parent: "integration-guides"
    name: "Docker"
---

Many of our demos rely on [Docker] and [Docker Compose] to make it easy to deploy
Materialize, along with any other infrastructure the demo needs.

For the best experience using Docker, we recommend following the guidelines
outlined here.

### Installation

Follow the official instructions to install Docker and Docker Compose:

* [Install Docker](https://docs.docker.com/get-docker/)
* [Install Docker Compose](https://docs.docker.com/compose/install/)

### Increase Docker resources

Because many of our Docker-based demos leverage a large number of pieces of
infrastructure, we recommend running Docker with at least **2 CPUs** and
**8 GB** of memory.

On macOS:

1. Open **Docker for Mac**'s **Preferences** window.

1. Click **Resources**.

1. Click **Advanced**.

1. Move the **CPUs** slider to at least **2**.

1. Move the **Memory** slider to at least **8.00 GB**.

1. Click **Apply & Restart**.

Note that on Linux, Docker automatically shares memory with the host machines; as long as your host machine has more than 8 GB of memory, you shouldn't run into issues.

### Using Docker volumes

To persist the Materialize metadata, you can create a Docker volume and mount it to the [`/mzdata` directory](/cli/#data-directory) in the container:

```shell
# Create a volume
docker volume create --name mzdata
# Create a container with the volume mounted
docker run -v mzdata:/mzdata -p 6875:6875 materialize/materialized:{{< version >}} --workers 1
```

[Docker]: https://docs.docker.com/get-started/overview/
[Docker Compose]: https://docs.docker.com/compose/
[python]: https://www.python.org/downloads/
