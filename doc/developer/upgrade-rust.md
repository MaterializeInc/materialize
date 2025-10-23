# Upgrading Rust

Materialize builds with the [stable](https://rust-lang.github.io/rustup/concepts/channels.html)
release of Rust, which gets [updated every 6 weeks](https://releases.rs/). We try to pretty
aggressively track the latest version to get the newest features and make upgrades as easy as
possible.

Anyone is welcome to upgrade the version of Rust! Below is the list of things you need to do:

1. Pick a version of the Nightly Rust compiler. Materialize builds with the Stable compiler, but we
   run [Miri](https://github.com/rust-lang/miri) in CI which requires Nightly.
    * First, look at this list of [available components](https://rust-lang.github.io/rustup-components-history/aarch64-unknown-linux-gnu.html)
      and make sure you pick a version that has everything available.
    * Second, check the [rust-lang GitHub Issue Tracker](https://github.com/rust-lang/rust/issues?q=is%3Aopen%20label%3AP-critical)
      to make sure the Nightly version you pick does not have any open `P-critical` issues.
2. Bump the `rust-version` field in our [Workspace `Cargo.toml`](/Cargo.toml).
3. Bump the `NIGHTLY_RUST_DATE` value in the [`ci-builder`](/bin/ci-builder) script.
    * Note: CI has a nightly version of Rust so we can run [Miri](https://github.com/rust-lang/miri).
4. Locally run `rustup upgrade stable` to pull the latest version of the Rust toolchain, or
   whatever version you're upgrading to.
5. From the root of the repository run `cargo clippy --workspace --tests`, fix any new clippy lints
   that were introduced.
    * First try running `cargo fix`, that should go a long way in automatically fixing many of the lints.
6. Check if Rust's unicode version has changed. If it has make sure to include in the release notes
   what version it previously was, and what version it got bumped to.
    * The [Releases](https://github.com/rust-lang/rust/releases) page for the Rust repository
      should mention if it's been changed. But the only way to know for sure it to
      [git blame the `UNICODE_VERSION` const](https://github.com/rust-lang/rust/blame/master/library/core/src/unicode/unicode_data.rs).
7. **Before merging the PR**, run [Nightly](https://buildkite.com/materialize/nightly) to catch any performance
   regressions that may be caused by the upgrade. If there are minor performance regressions, it's most likely
   okay to proceed, but in general it's easier to make that decision while the PR is still open as
   opposed to merged on `main`.
8. When the upgrade PR finally merges, post in `#eng-general` to give everyone a heads up and let
   them know they can upgrade by running `rustup upgrade stable`.
