# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM openssh-static AS openssh

MZFROM distroless-prod-base

COPY environmentd entrypoint.sh /usr/local/bin/
COPY --from=openssh /output/ssh /usr/bin/ssh

# Antithesis symbolizes coverage from binaries linked under /symbols.
ARG ANTITHESIS
USER root
RUN ["/busybox/sh", "-c", "if [ -n \"$ANTITHESIS\" ]; then /busybox/mkdir -p /symbols && /busybox/ln -s /usr/local/bin/environmentd /symbols/environmentd; fi"]
USER materialize

# tini reaps orphaned subprocesses and forwards signals to the whole process
# group, including environmentd. Run entrypoint.sh through busybox `sh`
# explicitly, as distroless has no /bin/sh.
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/busybox/sh", "/usr/local/bin/entrypoint.sh"]
