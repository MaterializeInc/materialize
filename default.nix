let

pkgs = import (builtins.fetchTarball {
  name = "nixos-19-03-pre";
  url = https://github.com/nixos/nixpkgs/archive/5c52b25283a6cccca443ffb7a358de6fe14b4a81.tar.gz;
  sha256 = "0fhbl6bgabhi1sw1lrs64i0hibmmppy1bh256lq8hxy3a2p1haip";
}) {};

confluent = pkgs.callPackage ./confluent.nix { pkgs = pkgs; };

in pkgs.stdenv.mkDerivation rec {
  name = "materialize";
  buildInputs = with pkgs; [
    openssl
    pkgconfig
    rustup

    curl # for testing

    postgresql # only for psql
    less # for psql

    confluent
    darwin.apple_sdk.frameworks.Security
    darwin.apple_sdk.frameworks.CoreServices
  ];
}
