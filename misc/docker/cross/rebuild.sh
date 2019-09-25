#!/usr/bin/env bash
local_dir=/usr/app/
source_dir=/usr/src/materialize

docker run --user "$(id -u)":"$(id -g)" \
    -v "$PWD":$local_dir -w $source_dir \
    materialize/local-build \
    bash -c "mkdir -p $local_dir ;
cp -r target $local_dir/target/linux ;
cd $local_dir;
cargo build --release --target-dir=target/linux"
