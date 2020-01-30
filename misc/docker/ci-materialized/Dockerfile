# Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

FROM ubuntu:bionic

COPY materialized /usr/local/bin
COPY bootstrap.sql /usr/local/etc/materialized/

ENTRYPOINT ["materialized"]
