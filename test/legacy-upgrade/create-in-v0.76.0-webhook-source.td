# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ skip-if
SELECT mz_version_num() < 7600;

> CREATE CLUSTER webhook_cluster REPLICAS (r1 (SIZE 'scale=1,workers=1'));

> CREATE SOURCE my_webhook_source IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT TEXT
  INCLUDE HEADERS
  CHECK (
    WITH(HEADERS, BODY, SECRET secret1)
    decode(headers->'x-signature', 'base64') = hmac(body, secret1, 'sha256')
  )

$ webhook-append name=my_webhook_source x-signature=VNCe6bTKrlFO46GfiUYR/xFpeZ2H/KbLfR9oJKYAwkc=
using an mz secret

$ webhook-append name=my_webhook_source x-signature=invalid_signature status=400
this should not get appended
