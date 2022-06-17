# mzbuild

mzbuild is an build and orchestration system for [Docker] containers.

As a user or developer, you'll interact with mzbuild through three commands:

  * [**`mzcompose`**](#mzcompose) is a thin layer on top of [Docker Compose]
    that automatically downloads cached images from Docker Hub if available, or
    otherwise builds them locally if you've made changes to the inputs of the
    image.

    This approach keeps things snappy when running a demo or test on the latest
    tip of main, while ensuring that you don't need to modify the Docker
    Compose configuration as you make changes to the source code.

  * [**`mzimage`**](#mzimage) is a lower-level command that allows inspection
    and finer-grained control over the build process for the images in the
    repository.

From the root of the repository, invoke the commands as `bin/mzcompose` and
`bin/mzimage`, respectively. Any directory with an `mzcompose.yml` file will
also have a convenience script alongside it, which you can invoke as
`./mzcompose` from within that directory.

**Table of contents:**

  * [Tutorial](#tutorial)
    * [`mzimage`](#mzimage)
    * [`mzcompose`](#mzcompose)
  * [Input addressability](#input-addressability)
  * [Development](#development)
  * [Reference](#reference)
    * [`mzbuild.yml`](#mzbuildyml)
    * [`mzcompose.yml`](#mzcomposeyml)
    * [mzbuild `Dockerfile`](#mzbuild-Dockerfile)
  * [Motivation](#motivation)
    * [Why a new build system?](#why-a-new-build-system)
    * [Why Docker?](#why-docker)

## Tutorial

*Warning: mzbuild does not yet return particularly friendly error messages. This
is under active improvement.*

### `mzimage`

The core object of mzbuild is the Docker image. To ask mzbuild to manage a
Docker image for you, simply create a new directory in the repository with
a `Dockerfile` and a configuration file named `mzbuild.yml`.

Here's a simple example for a fictional Python load generator called
`fancy-load`:

```Dockerfile
# test/fancy/loadgen/Dockerfile

MZFROM ubuntu-base

RUN apt-get update && apt-get install -qy python3

COPY fancy-loadgen.py .

ENTRYPOINT ["python3", "fancy-loadgen.py"]
```

```yml
# test/fancy/loadgen/mzbuild.yml

name: fancy-loadgen
```

```py
# test/fancy/loadgen/fancy-loadgen.py

print("fancy load")
```

That's it! mzbuild will now automatically detect this image. You can see for
yourself with the `mzimage list` command:

```shell
$ bin/mzimage list
...
fancy-loadgen
...
```

You can then also ask `mzimage` to run the image:

```shell
$ bin/mzimage run fancy-loadgen
==> Acquiring materialize/fancy-loadgen:7J7C34QTCEKQTI5G3NDIH7FFMMWZ3IWB
$ docker pull materialize/fancy-loadgen:7J7C34QTCEKQTI5G3NDIH7FFMMWZ3IWB
...
Successfully tagged materialize/fancy-loadgen:7J7C34QTCEKQTI5G3NDIH7FFMMWZ3IWB
$ docker run -it --rm --init materialize/fancy-loadgen:7J7C34QTCEKQTI5G3NDIH7FFMMWZ3IWB
fancy load
```

Notice that random string of characters, `7J7C34QTCEKQTI5G3NDIH7FFMMWZ3IWB`, in
the output? That's the image's fingerprint, which is a Base32-encoded SHA-1 hash
of the inputs to your image. By default, mzbuild assumes all files in the
image's directory are inputs, which is usually a sane default.

Don't worry if your fingerprint isn't identical. A single stray newline will
result in a completely different fingerprint.

If you run the image again, since we've already got an image for that
fingerprint available, `mzimage` won't need to rebuild the image, and the
command should be much speedier:

```shell
$ bin/mzimage run fancy-loadgen
$ docker run -it --rm --init materialize/fancy-loadgen:7J7C34QTCEKQTI5G3NDIH7FFMMWZ3IWB
fancy load
```

Now try changing the Python script to print some fancier load:

```py
import sys
sys.stdout.buffer.write("🎩 load\n".encode("utf-8"))
```

If we ask `mzimage` about the fingerprint, we'll see it's changed to reflect
the new Python code:

```shell
$ bin/mzimage fingerprint fancy-loadgen
TPOXKNHJOZBEYRN635UXLDHML6INVVMV
```

We can also add dependencies on other images to fancy-loadgen. Let's say we
also want to include the billing-demo image. Edit the Dockerfile to look like
this:

```dockerfile
# test/fancy/loadgen/Dockerfile

MZFROM billing-demo AS billing-demo

MZFROM ubuntu-base

RUN apt-get update && apt-get install -qy python3

COPY fancy-loadgen.py .

COPY --from=billing-demo /usr/local/bin/billing-demo /usr/local/bin/billing-demo

CMD ["python3", "fancy-loadgen.py"]
```

And let's verify that the new image contains `billing-demo` where we expect it:

```shell
$ bin/mzimage run fancy-loadgen ls -lh /usr/local/bin/billing-demo
==> Acquiring materialize/billing-demo:FM4STU42G7W44OLAPKZNEZWGEPTMIVE6
$ docker pull materialize/billing-demo:FM4STU42G7W44OLAPKZNEZWGEPTMIVE6
FM4STU42G7W44OLAPKZNEZWGEPTMIVE6: Pulling from materialize/billing-demo
...
docker.io/materialize/billing-demo:FM4STU42G7W44OLAPKZNEZWGEPTMIVE6
==> Acquiring materialize/fancy-loadgen:LLJS6MMDVOHZJQFBP42DFBW6Z5N3CW4F
$ docker pull materialize/fancy-loadgen:LLJS6MMDVOHZJQFBP42DFBW6Z5N3CW4F
Error response from daemon: manifest for materialize/fancy-loadgen:LLJS6MMDVOHZJQFBP42DFBW6Z5N3CW4F not found: manifest unknown: manifest unknown
$ docker build --pull -f - -t materialize/fancy-loadgen:LLJS6MMDVOHZJQFBP42DFBW6Z5N3CW4F test/fancy/loadgen
...
Successfully tagged materialize/fancy-loadgen:LLJS6MMDVOHZJQFBP42DFBW6Z5N3CW4F

-rwxr-xr-x 1 root root 8.3M Apr 15 01:34 /usr/local/bin/billing-demo
```

Notice how mzbuild automatically downloaded a copy of the billing-demo from
Docker Hub! Seamless.

### `mzcompose`

To create an mzcompose configuration that uses the `fancy-loadgen` image we
built in the previous tutorial, just drop an `mzcompose.yml` file into a
directory:

```yml
version: "3.7"

services:
  fancy:
    mzbuild: fancy-loadgen
```

If you're unfamiliar with Compose, you may want to take a look at the
[Compose file reference][compose-ref] for details.

Now bring the configuration up with `bin/mzcompose`:

```shell
$ bin/mzcompose --find fancy up
==> Collecting mzbuild dependencies
materialize/billing-demo:FM4STU42G7W44OLAPKZNEZWGEPTMIVE6
materialize/fancy-loadgen:Z2GPU4TQMCV2PGFTNUPYLQO2PQAYD6OY
==> Delegating to Docker Compose
Starting fancy_fancy_1 ... done
Attaching to fancy_fancy_1
fancy_1  | 🎩 load
fancy_fancy_1 exited with code 0
```

The argument you pass to `--find` is the name of the directory containing the
`mzcompose.yml`. Don't worry: if this directory name is not unique across the
entire repository, `mzcompose` will complain.

Notice how `mzcompose` automatically acquired images for not just
`fancy-loadgen` but all of its dependencies before delegating to Docker Compose
to actually start the containers.

Typing that entire command out is painful, though. Let's ask mzcompose to
generate a convenience script for us:

```shell
$ bin/mzcompose gen-shortcuts
```

Now we can run `./mzcompose` from within the `test/fancy` directory:

```shell
cd test/fancy
$ ./mzcompose ps
==> Collecting mzbuild dependencies
materialize/billing-demo:FM4STU42G7W44OLAPKZNEZWGEPTMIVE6
materialize/fancy-loadgen:Z2GPU4TQMCV2PGFTNUPYLQO2PQAYD6OY
==> Delegating to Docker Compose
    Name                Command            State    Ports
---------------------------------------------------------
fancy_fancy_1   python3 fancy-loadgen.py   Exit 0
```

Let's add another mzbuild dependency, this time on `environmentd`:

```yml
version: "3.7"

services:
  fancy:
    mzbuild: fancy-loadgen
  environmentd:
    mzbuild: environmentd
```

`mzcompose` will automatically acquire the new dependency on the next
invocation. Note that if you have local changes to any Rust code, you'll likely
want to stash them away now, or `mzcompose` will be spending a lot of time
recompiling a fresh version of the `environmentd` image.

```shell
$ ./mzcompose up
==> Collecting mzbuild dependencies
materialize/billing-demo:FM4STU42G7W44OLAPKZNEZWGEPTMIVE6
materialize/fancy-loadgen:Z2GPU4TQMCV2PGFTNUPYLQO2PQAYD6OY
materialize/environmentd:EYBAS3HTGQS2SAVO3RBR5JS6AVGVRPJM
==> Acquiring materialize/environmentd:EYBAS3HTGQS2SAVO3RBR5JS6AVGVRPJM
$ docker pull materialize/environmentd:EYBAS3HTGQS2SAVO3RBR5JS6AVGVRPJM
EYBAS3HTGQS2SAVO3RBR5JS6AVGVRPJM: Pulling from materialize/environmentd
...
Status: Downloaded newer image for materialize/environmentd:EYBAS3HTGQS2SAVO3RBR5JS6AVGVRPJM
docker.io/materialize/environmentd:EYBAS3HTGQS2SAVO3RBR5JS6AVGVRPJM
==> Delegating to Docker Compose
Starting fancy_fancy_1        ... done
Creating fancy_environmentd_1 ... done
Attaching to fancy_fancy_1, fancy_environmentd_1
fancy_1         | 🎩 load
environmentd_1  | environmentd: '--workers' must be specified and greater than 0
environmentd_1  | hint: As a starting point, set the number of threads to half of the number of
environmentd_1  | cores on your system. Then, further adjust based on your performance needs.
environmentd_1  | hint: You may also set the environment variable MZ_WORKERS to the desired number
environmentd_1  | of threads.
fancy_fancy_1 exited with code 0
fancy_environmentd_1 exited with code 1
```

And that's it. Pretty simple. Note that you can add normal `image` services to
your `mzcompose.yml`, too. That works just as it would in vanilla Docker
Compose.

```yml
version: "3.7"

services:
  fancy:
    mzbuild: fancy-loadgen
  environmentd:
    mzbuild: environmentd
  zookeeper:
    image: zookeeper:3.4.13
```

#### Release vs development builds

Via `mzbuild`, `mzcompose` supports building binaries in either release or
development mode. By default, binaries are built using release mode. You can
choose dev mode instead by passing the `--dev` flag to `mzcompose`:

```shell
$ bin/mzcompose --dev --find fancy up
```

## Input addressability

mzbuild is an *input-addressable* build system.

The key insight is that most Docker images are designed to be a pure function
from a set of input files to a packaged image. Images generally only need to be
built once for a given set of inputs.

In practice, Docker build processes tend to depend on non-reproducible state,
like APT repositories or the current time, but these typically don't have a
meaningful impact on the build, and we are happy to ignore these annoyances for
now.

mzbuild uses SHA-1 hashes for its fingerprints, like Git. To prevent confusing a
fingerprint for a Git commit SHA, mzbuild fingerprints are encoded in uppercase
[Base32]. (Base32 is a bit easier to handle than Base64, as it doesn't include
any non-alphanumeric characters.)

## Development

mzbuild and associated tools are written in Python 3 and live in
[misc/python/materialize](/misc/python/materialize).

Their only dependency is Python 3.5+, which is easy to find or pre-installed on
most Linux distributions, and pre-installed on recent versions of macOS, too.
Python dependencies are automatically installed into a virtualenv by the
[pyactivate wrapper script](/bin/pyactivate).

Using Python 3.6 would be a good bit more convenient, but our CI image runs on
Ubuntu 16.04, which is still shipping Python 3.5. Supporting the oldest Ubuntu
LTS release seems like a decent baseline, anyway.

Integration tests for `mzcompose` are in [`test/mzcompose`](/test/mzcompose).

## Reference

### mzbuild.yml

An `mzbuild.yml` file describes how to build a Docker image from a `Dockerfile`
and a pre-`docker build` plugin.

The directory containing a `mzbuild.yml` file is called the "mzbuild context."

#### Example

```yml
name: environmentd
pre-image:
  - type: cargo-build
    bin: environmentd
    strip: false
publish: true
```

#### Fields

* `name` (string, required) is an identifier for the image. It must be unique
  within the repository. If the image is publishable, it will be published to
  Docker Hub as `materialize/<name>`.

* `pre-image` (list of maps) specifies plugins to run *before* invoking `docker
  build`. The plugins are run in order. This is where the magic happens for
  Rust code.

  At the moment `pre-image` only supports three plugins:

  * `type: copy` recursively copies the contents of a directory into the mzbuild
     context.

     The `source` field specifies the directory from which files should be
     copied. It is relative to the root of the repository. The `destination`
     field specifies the directory into which files should be copied. It is
     relative to the mzbuild context. Both fields are required.

     The name of the file in the `destination` directory will be the name of the
     file in the `source` directory with the `source` prefix removed. So a file
     named `/path/to/source/a/b/c.ext` will be copied into
     `/path/to/destination/a/b/c.ext`.

     The optional `matching` field specifies a glob that determines which
     files in the `source` directory to copy.

  *  `type: cargo-bin` builds a Rust binary with Cargo. The `bin` field is a
     string or a list of strings that indicates the name of one or more binary
     target in the Cargo workspace to build. The resulting artifact will be
     placed into the mzbuild context. The `example` field works identically but
     names an example to build rather than a binary.

     All files within the crate directory, and all files within the directories
     of any transitive _path_ dependencies of the crate (i.e., dependencies in
     this workspace, rather than on crates.io), will be considered as additional
     inputs to the build, plus the top-level `Cargo.toml`, `Cargo.lock`, and
     `.cargo/config` files.

     Cargo is invoked with the `--release` flag unless the `--dev` flag is
     specified. The binary will be stripped of debug information unless
     `strip: false` is requested.

     In rare cases, it may be necessary to extract files from the build
     directory of a dependency. The `extract` key specifies a mapping from a
     dependent package to a mapping from source files and directories to
     destination directories. Source paths are interpreted relative to that
     crate's build directory while destination paths are interpreted relative to
     the build context. Note that `extract` is only relevant if the dependency
     has a custom Cargo build script, as Rust crates without a build script do
     not have a build directory.

  * `type: cargo-test` builds a special image that simulates `cargo test`. This
     plugin is very special-cased at the moment, and unlikely to be generally
     useful.

* `publish` (bool) specifies whether the image should be automatically published
  to Docker Hub by CI. Non-publishable images can still be *used* by users and
  CI, but they must always be built from source. Use sparingly. The default is
  `true`.

* `build-args` (map[str, str]) a list of parameters to pass as [`--build-arg`][buildarg]
  to Docker. For example:

  ```yaml
  name: example
  build-args:
    VERSION: '1.0'
  ```

[buildarg]: https://docs.docker.com/engine/reference/commandline/build/#set-build-time-variables---build-arg

#### Build artifacts

When using a `pre-image` plugin, arbitrary build artifacts will be copied into
the mzbuild context. Be sure to add a `.gitignore` to the mzbuild context and
ignore these files! Ignored files will be excluded from the mzbuild fingerprint,
and will be automatically deleted at the beginning of the pre-image phase to
ensure idempotent builds.

### mzcompose.yml

An mzcompose configuration file is a small extension to the [Docker Compose
configuration file][compose-ref]. All extensions apply to the `services`
top-level map.

#### Example

```yaml
version: "3.7"

services:
  environmentd:
    mzbuild: environmentd
    propagate_uid_gid: true
```

#### Fields

* `mzbuild` (string) indicates that the service's image should be dynamically
  acquired by mzcompose prior to invoking Docker Compose. The value must match
  the name of an image in the repository.

  If `mzbuild` is specified, neither of the standard properties `build` nor
  `image` should be specified.

* `propagate_uid_gid` (bool) requests that the Docker image be run with the user
  ID and group ID of the host user. It is equivalent to passing `--user $(id
  -u):$(id -g)` to `docker run`. The default is `false`.

### mzbuild Dockerfile

An mzbuild Dockerfile is like a [normal Dockerfile][dockerfile-ref], but it can depend on other
mzbuild images.

#### Example

```dockerfile
MZFROM environmentd

MZFROM ubuntu-base

COPY --from=0 ...
```


#### Commands

* `MZFROM <string> [AS <name>]` sets the base image for the build stage to the
  specified mzbuild image. It is like the vanilla Dockerfile [FROM
  command][dockerfile-from], except that the image named must be a valid mzbuild
  image in the repository, not a vanilla Docker image.

## Motivation

### Why a new build system?

End-to-end tests of Materialize can involve orchestrating a dozen different
services: ZooKeeper, Kafka, PostgreSQL, Prometheus, Grafana, load simulators,
and so on. So far, the most workable solution for managing such a dizzying array
of services has involved Docker and Docker Compose.

Docker has its shortcomings, but its popularity means it is more widely used and
understood than any other tool we're aware of. Many developers have a passing
familiarity with Docker. Empirically, many users of Materialize are willing to
download Docker—or already have it installed—in order to take Materialize for a
spin, and some prospective customers have explicitly requested the Docker
distribution channel. The situation is similar with Docker Compose: it's not
perfect, but it gives us a lot of power for free.

If you are not sold on Docker, see the [Why docker?](#why-docker) section below.

Our single biggest pain point with Compose has been the inability to seamlessly
share Compose configurations between our developers, CI, and downstream users
running demos. Developers want the Compose files to build all images from
source, so that changes made locally are reflected in the containers. Users want
Compose files to download images from Docker Hub, since building the images from
source can take the better part of an hour, if they even have a build toolchain
available. And CI wants both, as it needs to build from source on one machine,
then distribute those pre-built images to downstream workers.

Compose, however, requires you to commit to an option for every service. Either
a) a service is built from source or, b) it is downloaded from Docker Hub, and
there is no in between.

To make matters worse, even if Compose had the desired behavior, building Rust
inside of a Dockerfile-managed build process would take upwards of 10m. Since
Docker builds start from a clean sandbox on every run, the Cargo cache is
freezing cold, and all dependencies must be compiled from scratch. Instead, we
need to build the Rust code outside of the Docker build process on a host
machine with a warm Cargo cache, and then copy the built binary into the Docker
build context.

To work around these limitations, our codebase has grown an increasingly complex
collection of shell scripts and Buildkite configurations. Aside from the
maintainability concerns, there is also a serious usability concern: every demo
has a slightly different interface to run it, configure it, test changes to it,
and deploy it.

So since we don't want to give up on Docker and Compose, the best option seemed
to be a thin wrapper script on top of Compose that acquired all the necessary
Docker images, using whatever means necessary, and then delegated to Compose
once all the images were in place. That wrapper script is mzbuild.

### Why Docker?

Our requirements for a demo and testing tool are as follows:

  * Support for macOS and Linux.
    * Ideally, the platform differences would be entirely handled by the tool,
      and invisible to those using it.
  * Automatic downloading and installation of myriad services.
  * Version pinning, so that everyone runs the same version of the services.
  * Automatic configuration of those services to talk to one another.
  * High probability that if a demo or test works on one machine, it will
    work on another.
  * Easy cleanup, so that data generated by the demo or test does not persist
    permanently on the user's machine.

Docker and Docker Compose satisfy all of the above criteria. The (fair)
criticisms are that the tools are complicated, slow, and hard to understand and
debug, but so far it seems the tradeoff seems worth it.

The closest competitors to Docker and Compose are [Kubernetes] and [Helm], but
these are *more* complicated than Docker and Compose, as they're more focused on
the enterprise-grade production deployment scenario.

[Base32]: https://en.wikipedia.org/wiki/Base32
[compose-ref]: https://docs.docker.com/compose/compose-file/
[Docker Compose]: https://docs.docker.com/compose/
[Docker]: https://www.docker.com
[dockerfile-from]: https://docs.docker.com/engine/reference/builder/#from
[dockerfile-ref]: https://docs.docker.com/engine/reference/builder/
[Kubernetes]: https://kubernetes.io
[Helm]: https://helm.sh
