{ pkgs ? import <nixpkgs> {} }:

let confluent = pkgs.callPackage ./confluent.nix { pkgs = pkgs; };

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
  ];
}
