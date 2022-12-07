# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM ubuntu-base

RUN apt-get update && apt-get -qy install ca-certificates

COPY examples/persistcli /usr/local/bin/persistcli

# TODO: the intention is for this image to eventually serve as the one-off
# job launcher for all of materialize, not just for persist. but because of
# short-term reasons, the entrypoint for now is just into persistcli.

ENTRYPOINT ["persistcli"]
