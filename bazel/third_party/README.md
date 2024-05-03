# Third-party Dependencies

This folder contains Bazel rules for building external/third-party dependencies that require
"special treatment". For example we want to statically link `openssl` so our builds are hermetic,
but `openssl` requires a C/C++ toolchain and Perl. Here we define the rules for including those
toolchains and any other steps required.
