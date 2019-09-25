#!/usr/bin/env bash

# I'm sorry

set -e

if [[ ! -f Cargo.tom ]] ; then
    echo "This must be run from the materialize directory like so:"
    echo "    bash misc/docker/cross/build.sh"
    exit 1
fi

crossdir=misc/docker/cross

mkdir -p $crossdir/deps

# TODO: we should pull the exact version of avro-rs from the lockfile?
for repo in avro-rs rust-rdkafka futures-rs sqlparser ; do
    if [[ ! -d $crossdir/deps/$repo ]]; then
        git clone --recurse-submodules ssh://git@github.com/MaterializeInc/$repo.git $crossdir/deps/$repo
    fi
done

(
    cd $crossdir/deps/futures-rs
    # the exact commit in Cargo.lock
    git checkout 29d0d2af30620039b79f77441a82f6dbf8e0cf66
)

# the Dockerfile COPY's avro into the /deps/ dir, so that's the path we use
sed -i '' -Ee 's,git.=.*/([^.]+).git.*,path = "/deps/\1" },' Cargo.toml

if ! (grep -E '^\[patch.*sqlparser' Cargo.toml>/dev/null) ; then
    cat >>Cargo.toml <<EOF
[patch."ssh://git@github.com/MaterializeInc/sqlparser.git"]
sqlparser = { path = "/deps/sqlparser" }
EOF
fi

docker build -f $crossdir/Dockerfile . -t materialize/local-build
