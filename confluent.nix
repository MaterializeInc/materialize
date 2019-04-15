{ pkgs ? import <nixpkgs> {} }:

pkgs.stdenv.mkDerivation rec {
  name = "confluent";
  src = fetchTarball {
    url = http://packages.confluent.io/archive/5.2/confluent-5.2.1-2.12.tar.gz;
    sha256 = "1y8b24wdl98wdpqq2m0p7wcgj4pij6xqfz7c7hfqcl80r03n3ihv";
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
