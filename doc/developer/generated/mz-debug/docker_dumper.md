---
source: src/mz-debug/src/docker_dumper.rs
revision: 4487749289
---

# mz-debug::docker_dumper

Implements `DockerDumper`, which implements the `ContainerDumper` trait for Docker-based (emulator) environments.
Collects container logs, `docker inspect` output, `docker stats`, and `docker top` into the debug output directory, retrying each command up to a 30-second timeout via `mz_ore::retry`.
Also exposes `get_container_ip`, which reads a container's IP address and is called during emulator context initialization in `main.rs`.
