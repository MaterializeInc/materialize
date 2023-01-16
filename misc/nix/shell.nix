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

let
    target-analyzer = builtins.toString ./../../.rust-analyzer/target;
    scoped-rust-analyzer = pkgs.writeShellScriptBin "rust-analyzer" ''
      exec ${pkgs.coreutils}/bin/env CARGO_TARGET_DIR=${target-analyzer} ${pkgs.rust-analyzer}/bin/rust-analyzer
    '';
in
stdenv.mkDerivation rec {
  name = "materialize";
  buildInputs = with pkgs; [
      clang_14
      cmake
      rustup
      openssl
      postgresql
      pkg-config
      lld_14
      python39
      scoped-rust-analyzer
  ];

  hardeningDisable = [ "fortify" ];

  RUSTFLAGS = "-Clinker=clang -Clink-arg=--ld-path=${pkgs.mold}/bin/mold -Clink-arg=-Wl,--warn-unresolved-symbols -Cdebuginfo=1 -Csymbol-mangling-version=v0";
}
