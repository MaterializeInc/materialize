---
title: "Materialize CLI Installation"
description: "The Materialize CLI can be installed through several different methods."
menu:
  main:
    parent: cli
    name: Installation
    weight: 1
---

We offer several installation methods for `mz` on macOS and Linux.

## macOS

On macOS, we we recommend using Homebrew.

### Homebrew

You'll need [Homebrew] installed on your system. Then install `mz` from
[our tap][homebrew-tap]:

```shell
brew install materializeinc/materialize/mz
```

### Binary download

```shell
curl -L https://binaries.materialize.com/mz-latest-$(uname -m)-apple-darwin.tar.gz \
    | sudo tar -xzC /usr/local --strip-components=1
```

## Linux

On Linux, we recommend using APT, if supported by your distribution.

### apt (Ubuntu, Debian, or variants)

```shell
curl -fsSL https://dev.materialize.com/apt/materialize.sources | sudo tee /etc/apt/sources.list.d/materialize.sources
sudo apt update
sudo apt install materialize-cli
```

### Binary download

```shell
curl -L https://binaries.materialize.com/mz-latest-$(uname -m)-unknown-linux-gnu.tar.gz \
    | sudo tar -xzC /usr/local --strip-components=1
```

## Docker

You can use the `materialize/mz` Docker image to run `mz` on any platform that
is supported by Docker. You'll need to mount your local `~/.mz` directory in the
container to ensure that configuration settings and authentiation tokens outlive
the container.

```
docker run -v $HOME/.mz:/root/.mz materialize/mz [args...]
```

[Homebrew]: https://brew.sh
[homebrew-tap]: https://github.com/MaterializeInc/homebrew-materialize
