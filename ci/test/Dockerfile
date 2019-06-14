# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

FROM rust:1.35

RUN curl -fsSL https://github.com/benesch/autouseradd/releases/download/1.1.0/autouseradd-1.1.0-amd64.tar.gz \
    | tar xz -C /usr --strip-components 1

RUN rustup component add clippy rustfmt

COPY --from=materialize/sccache:0.2.8 /usr/local/bin/sccache /usr/local/bin/sccache

RUN apt-get update && apt-get install -y clang libclang-dev

ENTRYPOINT ["autouseradd", "--user", "materialize"]
