# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM ubuntu-base

# We install sqlite3 to allow debugging of the catalog file, if necessary.
RUN apt-get update && apt-get -qy install ca-certificates curl sqlite3

RUN curl -fsSL "https://github.com/krallin/tini/releases/download/v0.19.0/tini" > /usr/bin/tini \
    && echo "93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c  /usr/bin/tini" | sha256sum --check \
    && chmod +x /usr/bin/tini

COPY kdestroy kinit coordd /usr/local/bin/

ENTRYPOINT ["tini", "--", "coordd"]
