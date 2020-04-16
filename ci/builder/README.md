# ci-builder

This directory contains the configuration to build a Docker image called the "CI
builder." The image contains the dependencies required to build, test, and
deploy all of the code in this repository.

All CI jobs run their commands inside of this image. The version of the image to
use is checked into the repository. This makes it trivial to update or add
software to CI: just push a new version of the image to Docker Hub, then land a
PR that points CI at the new version of the image.

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

You can test the image locally by modifying the [stable.stamp] file in this
directory to have the contents "latest". Then run

```shell
$ bin/ci-builder run stable
```

and you'll be dropped into a Bash shell inside a CI builder container based on
your latest changes. You can also run a command other than Bash directly:

```shell
$ bin/ci-builder run stable echo "hello from abroad"
```

When you're happy with your changes, push them to Docker Hub:

```shell
$ bin/ci-builder push stable
```

(If you're feeling lucky, you can skip the build/test phase and push directly to
Docker Hub. CI will verify that everything is working, after all.)

Don't worry about pushing a broken image to Docker Hub; you can push with
abandon. Those images won't get used unless you land a change to stamp.stamp
that references the new tag. The script is careful to make tags sufficiently
unique that you don't need to worry about overwriting existing tags on Docker
Hub.

After a successful push, the script will update stable.stamp with the new tag of
the image. Commit the resulting diff, *including* the changes to the Dockerfile
that were incorporated into the built image, and open a PR!

## Upgrading the Rust version

1. Update the [`rust-toolchain`] file with the desired version.

2. Run:

   ```shell
   bin/ci-builder push stable
   ```

3. Run `bin/check` and `bin/lint` to see if there are any new Clippy lints or
   rustfmt style adjustments in this release. If so, fix them.

4. Commit all outstanding changes and open a PR. Be sure to include the
   auto-generated update to [stable.stamp].

You may also need to upgrade the nightly version of the image, if it has become
sufficiently out-of-date that it can no longer compile our codebase. That
process is even easier:

1. Run:

   ```shell
   $ bin/ci-builder push nightly
   ```

2. Commit the outstanding change to [nightly.stamp] and open a PR.

Sometimes nightly versions are missing components that we depend on, like Clippy
or miri. If that happens, you'll get a somewhat cryptic error message, like
"unknown component: miri." The trick is to find a nightly version that has all
the required compoents using the [component history tracker][rust-toolstate].
When you've found a date where all required components are present for the
x86\_64-unknown-linux-gnu target, you can request the nightly toolchain from
that date like so:

```shell
$ bin/ci-builder push nightly-2001-02-03
```

[bin/ci-builder]: /bin/ci-builder
[rust-toolchain]: /rust-toolchain
[stable.stamp]: stable.stamp
[nightly.stamp]: nightly.stamp
[rust-toolstate]: https://rust-lang.github.io/rustup-components-history/
