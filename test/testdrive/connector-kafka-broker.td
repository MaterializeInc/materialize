# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# Test basic connector functionality

> CREATE CONNECTOR testconn
  FOR KAFKA BROKER 'kafka:1234'
  WITH (security_protocol = 'SASL_SSL', sasl_mechanisms = 'PLAIN')

> SELECT name, connector_type from mz_connectors
name  connector_type
------------------------------
testconn   kafka

> SHOW CREATE CONNECTOR testconn
Connector   "Create Connector"
---------------------------------
materialize.public.testconn   "CREATE CONNECTOR \"materialize\".\"public\".\"testconn\" FOR KAFKA BROKER 'kafka:1234' WITH (\"security_protocol\" = 'SASL_SSL', \"sasl_mechanisms\" = 'PLAIN')"


> DROP CONNECTOR testconn
