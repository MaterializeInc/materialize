# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

let

pkgs = import (builtins.fetchTarball {
  name = "nixos-19.09";
  url = https://releases.nixos.org/nixos/19.09/nixos-19.09.976.c75de8bc12c/nixexprs.tar.xz;
  sha256 = "0jdi8ihkhhkglx3amkrkkrm4ac60xk2a1bs4mwkffz72lrrg16p0";
}) {};

demo_utils = pkgs.makeSetupHook { } ./doc/demo-utils.sh;

in pkgs.stdenv.mkDerivation rec {
  name = "materialize";
  buildInputs = with pkgs; [
    demo_utils
    openssl
    zlib.static
    clang
    llvmPackages.libclang
    cmake
    pkgconfig
    rustup
    lsof # for mtrlz-shell
    curl # for testing
    confluent-platform
    shellcheck
    ] ++
    (if stdenv.isDarwin then [
    darwin.apple_sdk.frameworks.Security
    darwin.apple_sdk.frameworks.CoreServices
    ] else []);
  shellHook = ''
    export LIBCLANG_PATH=${pkgs.llvmPackages.clang-unwrapped.lib}/lib
    export PATH=$(pwd)/bin/:$PATH
    source ${demo_utils}/nix-support/setup-hook
    ulimit -m $((8*1024*1024))
    ulimit -v $((8*1024*1024))
   '';
}
