# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that the first use of an unmaterialized source presents all data
# that was available at the time the source was created.
#
# See #2310, #10587, #11048, and #11072.

$ set-sql-timeout duration=0s force=true

# Test with an unmaterialized file source.

$ file-append path=file repeat=100000
data

> CREATE SOURCE file
  FROM FILE '${testdrive.temp-dir}/file'
  FORMAT TEXT

> SELECT count(*) FROM file
100000

# Test with an unmaterialized Avro OCF source.

$ avro-ocf-write path=file.ocf schema={"type": "string"} repeat=100000
"data"

> CREATE SOURCE file_ocf
  FROM AVRO OCF '${testdrive.temp-dir}/file.ocf'

> SELECT count(*) FROM file_ocf AS file_ocf (text)
100000

# Test with an unmaterialized Kafka source.

$ kafka-create-topic topic=data

$ kafka-ingest format=bytes topic=data repeat=100000
1

> CREATE SOURCE kafka
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT TEXT

> SELECT count(*) FROM kafka
100000
