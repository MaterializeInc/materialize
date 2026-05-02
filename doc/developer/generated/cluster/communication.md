---
source: src/cluster/src/communication.rs
revision: b0fa98e931
---

# communication

Establishes a fault-tolerant TCP or Unix socket mesh for a multi-process Timely cluster.
`initialize_networking` calls `create_sockets` in a retry loop to obtain a connected set of sockets, then delegates to Timely's `initialize_networking_from_sockets`.
`create_sockets` implements a generation-epoch protocol: process 0 mints a new epoch each startup; other processes adopt that epoch when connecting to lower-indexed peers and reject connections from earlier generations, preventing a "circle of doom" where crashed processes keep infecting recovering ones.
The protocol uses system time plus a random nonce as the epoch, so it is robust against non-monotonic clocks as long as time increases eventually.
