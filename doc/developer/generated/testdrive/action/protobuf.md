---
source: src/testdrive/src/action/protobuf.rs
revision: 5b2cefc829
---

# testdrive::action::protobuf

Implements the `protobuf-compile-descriptors` builtin command, which invokes `protoc` to compile `.proto` files into a binary descriptor file.
The `PROTOC` and `PROTOC_INCLUDE` environment variables override the default `protoc` binary and include path; path separators in file names are forbidden to prevent directory traversal.
The optional `set-var` argument reads the compiled descriptor and stores it as a hex-escaped byte string (`\x...`) in a named command variable.
