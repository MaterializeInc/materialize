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
STOP REPLICA;
CHANGE REPLICATION SOURCE TO SOURCE_HOST='${arg.mysql-replication-master-host}', SOURCE_PORT=3306, SOURCE_USER='root', SOURCE_PASSWORD='${arg.mysql-root-password}', SOURCE_AUTO_POSITION=1, MASTER_DELAY=${arg.mysql-replica-delay-in-sec};
START REPLICA;
