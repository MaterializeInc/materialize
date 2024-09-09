# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ mysql-connect name=mysql url=mysql://root@${arg.mysql-host} password=${arg.mysql-root-password}

$ mysql-execute name=mysql
USE public;

DROP TABLE IF EXISTS t3;
CREATE TABLE t3 (f1 INTEGER NOT NULL);

INSERT INTO t3 VALUES (1);
