# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

! CREATE CONNECTION IF NOT EXISTS invalid_host TO SSH TUNNEL (
    HOST 'invalid-ssh-bastion-host',
    USER 'mz',
    PORT 23
  ) WITH (VALIDATE);
contains:failed to lookup address information

! CREATE CONNECTION IF NOT EXISTS invalid_port TO SSH TUNNEL (
    HOST 'ssh-bastion-host',
    USER 'mz',
    PORT 24
  ) WITH (VALIDATE);
regex:failed to connect to the remote host: connect to host .* port 24: Connection refused

> CREATE CONNECTION IF NOT EXISTS invalid_user TO SSH TUNNEL (
    HOST 'ssh-bastion-host',
    USER 'invalid',
    PORT 23
  );

! VALIDATE CONNECTION invalid_user;
regex:failed to connect to the remote host: invalid@.*: Permission denied \(publickey,keyboard-interactive\)\.

> DROP CONNECTION invalid_user;

! VALIDATE CONNECTION thancred;
regex:failed to connect to the remote host: mz@.*: Permission denied \(publickey,keyboard-interactive\)\.
