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
    PORT 22
  ) WITH (VALIDATE);
contains:failed to connect to the remote host: Could not resolve hostname

! CREATE CONNECTION IF NOT EXISTS invalid_port TO SSH TUNNEL (
    HOST 'ssh-bastion-host',
    USER 'mz',
    PORT 23
  ) WITH (VALIDATE);
contains:failed to connect to the remote host: connect to host ssh-bastion-host port 23: Connection refused

> CREATE CONNECTION IF NOT EXISTS invalid_user TO SSH TUNNEL (
    HOST 'ssh-bastion-host',
    USER 'invalid',
    PORT 22
  );

! VALIDATE CONNECTION invalid_user;
contains:failed to connect to the remote host: invalid@ssh-bastion-host: Permission denied (publickey,keyboard-interactive).

> DROP CONNECTION invalid_user;

! VALIDATE CONNECTION thancred;
contains:failed to connect to the remote host: mz@ssh-bastion-host: Permission denied (publickey,keyboard-interactive).
