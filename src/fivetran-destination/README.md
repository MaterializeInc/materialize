# Materialize Fivetran Destination

This directory contains a [Fivetran] destination for Materialize, built using
[Fivetran's SDK][fivetran-sdk].

## End users

To use this destination as an end user of Materialize and Fivetran, log in to
your Fivetran account, add a new destination, and choose the "Materialize"
destination. The destination will be labeled as "Partner Built", indicating that
Materialize maintains and supports the destination, rather than Fivetran.

## Contributors

### Testing

To test code changes to the destination, run the test suite at
[test/fivetran-destination]. Consult the mzcompose.py file in that directory
for instructions.

### Binary distribution

To build the destination into a static Rust binary for distribution, first make sure you have
updated the `misc/fivetran-sdk` submodule, this is how we include the protobuf definitions for the
SDK. From the root of the Materialize repository run:

```shell
git submodule update --init --recursive misc/fivetran-sdk
```

Once you have the `fivetran-sdk` submodule updated you can build the binary. If you already have 
[`protoc`](https://grpc.io/docs/protoc-installation/) installed and part of your PATH run:

```shell
cargo build --release -p mz-fivetran-destination
```

Otherwise you can enable the `protobuf-src` feature to build `protoc` from the vendored source:

```shell
cargo build --release -p mz-fivetran-destination --features protobuf-src
```

Cargo will emit the built binary at
`ROOT/target/release/mz-fivetran-destination.`

### Docker image distribution

To build the destination into a Docker image, run the following from the root of
the repository:

```
bin/mzimage acquire fivetran-destination
```

Pre-built Docker images are available on Docker Hub:
<https://hub.docker.com/r/materialize/fivetrain-destination>

[fivetran]: https://fivetran.com
[fivetran-sdk]: https://github.com/fivetran/fivetran_sdk
[test/fivetran-destination]: ../test/fivetran-destination
