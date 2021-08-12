# libbsd

This directory contains the subset of [libbsd] that is relevant to PID file
management.

## Updating

To update to a new version of libbsd, bump the version number in `update.sh`
and then run the script.

Note that only the files in `src` are managed by the script. You may need to
adjust the `include/libutil.h` header by hand to account for changes to the
libbsd functions. You may also need to pull in additional C files and adjust
the `build.rs` build script accordingly.

If any of the function signatures change, you'll also need to update the
`extern` declarations in Rust. The set of functions is small enough that it's
not worth using bindgen.

[libbsd]: https://libbsd.freedesktop.org/wiki/
