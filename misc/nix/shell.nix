# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

with import <nixpkgs> {};

let
    target-analyzer = builtins.toString ./../../.rust-analyzer/target;
    scoped-rust-analyzer = pkgs.writeShellScriptBin "rust-analyzer" ''
      exec ${pkgs.coreutils}/bin/env CARGO_TARGET_DIR=${target-analyzer} ${pkgs.rust-analyzer}/bin/rust-analyzer
    '';
in
stdenv.mkDerivation rec {
  name = "materialize";
  buildInputs = with pkgs; [
      clang_13
      cmake
      rustup
      openssl
      postgresql
      pkg-config
      lld_13
      python39
      scoped-rust-analyzer
  ];

  hardeningDisable = [ "fortify" ];

  RUSTFLAGS = "-C linker=clang -C link-arg=--ld-path=${pkgs.mold}/bin/mold -C link-arg=-Wl,--warn-unresolved-symbols -C debuginfo=1";
}
