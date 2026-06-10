# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

{ pkgs ? import <nixpkgs> {} }:
with pkgs;

stdenv.mkDerivation {
  name = "materialize";
  buildInputs = with pkgs; [
    clang
    rustPlatform.bindgenHook
    cmake
    perl
    rustup
    pkg-config
    python3
    openssl
    bazel_7

    # CLI tools
    postgresql
    git
    docker

    # For docs
    hugo

    # For lint checks
    shellcheck
    buf
    nodejs_22
    jq
    openssh
  ] ++ lib.optionals stdenv.isDarwin [
    libiconv
    darwin.apple_sdk.frameworks.DiskArbitration
    darwin.apple_sdk.frameworks.Foundation
  ];

  RUSTFLAGS = "-Cdebuginfo=1 -Csymbol-mangling-version=v0"
    + lib.optionalString stdenv.isLinux
      " -Clinker=clang -Clink-arg=--ld-path=${pkgs.mold}/bin/mold -Clink-arg=-Wl,--warn-unresolved-symbols";
}
