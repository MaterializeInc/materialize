# ci-builder

This directory contains the configuration to build a Docker image called the "CI
builder." The image contains the dependencies required to build, test, and
deploy all of the code in this repository.

All CI jobs run their commands inside of this image. The definition of the image
is also stored in this repository. This makes it trivial to update or add
software to CI: just modify the `Dockerfile` defining the image.

Crucially, old commits that might be incompatible with the new software will
continue to use the old versions of the software, because they reference the old
CI builder image.

## Modifying the image

Make the desired modifications to the `Dockerfile` in this directory. Then use
the [bin/ci-builder] script to build a new version of the image:

```shell
$ bin/ci-builder build stable
```

The `stable` argument indicates what channel of Rust to include in the image,
either `stable` or `nightly`. Some CI commands need a nightly version of Rust
because they depend on unstable features.

Beware that building the image takes at least ten minutes, even on a beefy
machine.

Then run:

```shell
$ bin/ci-builder run stable
```

and you'll be dropped into a Bash shell inside a CI builder container based on
your latest changes. You can also run a command other than Bash directly:

```shell
$ bin/ci-builder run stable echo "hello from abroad"
```

When you're happy with your changes, open a PR. CI will build and push the
image to Docker Hub so that no one else needs to build the image from scratch.

## Acquiring root within the image

When debugging issues with the image, it is occasionally useful to acquire root.
You can't run `sudo` because the image does not have `sudo` installed.

Instead, you can use the `root-shell` command. While running the image in one
shell, open a new shell and run:

```shell
$ bin/ci-builder root-shell stable
```

That command will open a new root shell into the most recently launched
ci-builder image. It will also run `apt-get update` so that you can use `apt
install` to install any additional software you might need.

When using the root shell, beware that if you create files as root on any
shared volumes, those files will be owned by root *on your host machine*.

## Upgrading the Rust version

1. Update the workspace [Cargo.toml] file with the desired version.
2. Update your local stable Rust installation so that it can build with the new minimum version:

   ```
   rustup update
   ```

3. Run `bin/check` and `bin/lint` to see if there are any new Clippy lints or
   rustfmt style adjustments in this release. If so, fix them.

4. (optional) [Rebuild the image](#modifying-the-image)
5. Commit all outstanding changes and open a PR.

You may also need to upgrade the nightly version of the image, if it has become
sufficiently out-of-date that it can no longer compile our codebase. That
process is even easier: update the `NIGHTLY_RUST_DATE` at the top of
bin/ci-builder to the current date.

Sometimes nightly versions are missing components that we depend on, like Clippy
or miri. If that happens, you'll get a somewhat cryptic error message, like
"unknown component: miri." The trick is to find a nightly version that has all
the required components using the [component history tracker][rust-toolstate].

[bin/ci-builder]: /bin/ci-builder
[rust-toolstate]: https://rust-lang.github.io/rustup-components-history/
