# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

{ pkgs ? import <nixpkgs> {} }:

pkgs.stdenv.mkDerivation rec {
  name = "confluent";
  src = fetchTarball {
    url = http://packages.confluent.io/archive/5.3/confluent-5.3.1-2.12.tar.gz;
    sha256 = "1ij5x1qw486rbih7xh2c01s60c3zblj6ad1isf5y99sh47jcq76c";
  };
  buildInputs = with pkgs; [
    which curl
    makeWrapper
  ];
  propagatedBuildInputs = with pkgs; [
    jre8
  ];
  buildPhase = ":";   # nothing to build
  installPhase = ''
    cp -aR ./ $out/
    for file in $(ls $out/bin); do
      if [ -f $out/bin/$file ]; then
        wrapProgram $out/bin/$file \
          --set LOG_DIR /tmp/confluent/ \
          --suffix PATH : ${pkgs.which}/bin:${pkgs.curl}/bin
      fi
    done
  '';
}
