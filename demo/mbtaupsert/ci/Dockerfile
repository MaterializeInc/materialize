# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM ubuntu:bionic-20200403

RUN apt-get update && apt-get install -qy curl unzip

#Copy build and scripts to /usr/local/bin what about demo-configs?

COPY mbtaupsert /usr/local/bin
COPY mbta-demo-cmd.sh /usr/local/bin

#create workspace
WORKDIR /workdir
VOLUME [ "/workdir/archive", "/workdir/configs", "/workdir/workspace"]

ENTRYPOINT ["mbta-demo-cmd.sh"]
