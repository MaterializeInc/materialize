This directory is meant to generate TLS certificates for testing Kafka clusters
with SSL authentication, and then allow `mzbuild` to bundle them into a Docker
image.

1. Run `materialize/test/cert-gen-kafka/create-certs.sh`.
    - `create-certs.sh` requires both `keytool` and `openssl`.
1. Either:
    - Run `bin/mzimage build cert-gen-kafka`
    - Implicitly invoke it using `mzbuild: cert-gen-kafka` in your
      `mzcompose.yml` file

    Note that these commands will fail if you don't first run `create-certs.sh`
    to generate the `secrets` dir.
