FROM rust:1.37-stretch

WORKDIR /usr/src/materialize

RUN apt-get update -o Acquire::Languages=none \
    && apt-get install -qy --no-install-recommends \
        libclang-dev \
        llvm-dev \
        clang \
    && apt-get autoremove -qy \
    && rm -rf \
        /var/cache/apt/archives \
        /var/lib/apt/lists/* \
    ;


# TODO: fuzz shouldn't be required for this
COPY Cargo.lock Cargo.toml ./
COPY src ./src
COPY fuzz ./fuzz
# required for the build script
COPY .git ./.git
COPY misc/docker/cross/deps/ /deps/

RUN pwd && ls && \
    cargo build --release --bin materialized \
    ;
