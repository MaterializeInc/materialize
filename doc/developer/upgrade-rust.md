# Upgrading Rust

Materialize builds with the [stable](https://rust-lang.github.io/rustup/concepts/channels.html)
release of Rust, which gets [updated every 6 weeks](https://releases.rs/). We try to pretty
aggressively track the latest version to get the newest features and make upgrades as easy as
possible.

Anyone is welcome to upgrade the version of Rust! Below is the list of things you need to do:

1. Bump the `rust-version` field in our [Workspace `Cargo.toml`](/Cargo.toml) and in the [Bazel `WORKSPACE`](/WORKSPACE).
2. Bump the `NIGHTLY_RUST_DATE` value in the [`ci-builder`](/bin/ci-builder) script.
    * Note: CI has a nightly version of Rust so we can run [Miri](https://github.com/rust-lang/miri).
3. Locally run `rustup upgrade stable` to pull the latest version of the Rust toolchain, or
   whatever version you're upgrading to.
4. From the root of the repository run `cargo clippy --workspace --tests`, fix any new clippy lints
   that were introduced.
5. Check if Rust's unicode version has changed. If it has make sure to include in the release notes
   what version it previously was, and what version it got bumped to.
    * The [Releases](https://github.com/rust-lang/rust/releases) page for the Rust repository
      should mention if it's been changed. But the only way to know for sure it to
      [git blame the `UNICODE_VERSION` const](https://github.com/rust-lang/rust/blame/master/library/core/src/unicode/unicode_data.rs).
6. When the upgrade PR finally merges, post in `#eng-general` to give everyone a heads up and let
   them know they can upgrade by running `rustup upgrade stable`.
