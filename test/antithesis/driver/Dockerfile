# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM jobs

FROM python:3.12-slim-trixie@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b

COPY --from=0 /usr/local/bin/persistcli /usr/local/bin/persistcli
ARG ANTITHESIS
RUN if [ -n "$ANTITHESIS" ]; then mkdir /symbols && ln -s /usr/local/bin/persistcli /symbols/; fi

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY --from=registry.k8s.io/kubectl:v1.34.10@sha256:0c26218cafeff698ededa49656bf158e96a4d4f3b3744ebd552251a23b6a3924 /bin/kubectl /usr/local/bin/

# Antithesis catalogs the assertions of the Python sources in this directory.
COPY driver.py persist_audit.py /opt/antithesis/catalog/
COPY first_start /opt/antithesis/test/v1/driver/

CMD ["python3", "-u", "/opt/antithesis/catalog/driver.py"]
