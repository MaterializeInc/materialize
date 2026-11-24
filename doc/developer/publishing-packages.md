# Publishing packages

Several components of Materialize are distributed as packages for

## Docker images

Use [mzbuild](mzbuild.md) to build and push Docker images to the
[materialize](https://hub.docker.com/u/materialize) Docker Hub organization.
When you add a new publishable mzbuild image, you'll need an infrastructure
admin to create the Docker Hub repository—ask in #eng-infra if you don't have
the required permissions.

## Rust crates

Before publishing internal Rust crates to `crates.io`(https://crates.io/), be
sure to indicate that `MaterializeInc` is the reponsible maintainer by running
the following command:

```shell
cargo owner --add github:MaterializeInc:crate-owners
```

## PyPI and npm

The `Publish packages` GitHub Actions workflow (`.github/workflows/publish.yml`)
publishes the packages listed in `ci/deploy/pypi.py` and `ci/deploy/npm.py` on
every push to `main`, `v*.*` branches and `v*.*` tags, skipping versions that
are already released. It authenticates with trusted publishing from the
`release` environment, so there are no registry tokens. A new package must be
registered as a trusted publisher on PyPI or npm before its first release.

Use the [`materializeinc`](https://pypi.org/user/materializeinc/) PyPI user to
manage Materialize's Python packages on PyPI. Login information can be found in
the shared 1Password account.
