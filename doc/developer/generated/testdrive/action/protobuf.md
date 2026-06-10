---
source: src/testdrive/src/action/protobuf.rs
revision: e757b4d11b
---

# testdrive::action::protobuf

Implements the `protobuf-compile-descriptors` builtin command, which invokes `protoc` to compile `.proto` files into a binary descriptor file.
The `PROTOC` and `PROTOC_INCLUDE` environment variables override the default `protoc` binary and include path; path separators in file names are forbidden to prevent directory traversal.
