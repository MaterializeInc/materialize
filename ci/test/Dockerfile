# Copyright 2019 Timely Data, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Timely Data, Inc.

FROM rust:1.33

RUN curl -fsSL https://github.com/benesch/autouseradd/releases/download/1.1.0/autouseradd-1.1.0-amd64.tar.gz \
    | tar xz -C /usr --strip-components 1

RUN rustup component add clippy rustfmt

ENTRYPOINT ["autouseradd", "--user", "materialize"]
