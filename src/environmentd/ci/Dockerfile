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

# tini (from distroless-prod-base) is PID 1 so ssh tunnel subprocesses get
# reaped. entrypoint.sh runs environmentd, then sleeps on graceful exit. Run it
# through busybox `sh` explicitly, as distroless has no /bin/sh.
ENTRYPOINT ["/usr/bin/tini", "--", "/busybox/sh", "/usr/local/bin/entrypoint.sh"]
