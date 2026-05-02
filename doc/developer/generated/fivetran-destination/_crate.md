---
source: src/fivetran-destination/src/lib.rs
revision: 82d92a7fad
---

# mz-fivetran-destination

Implements a Fivetran destination connector that loads data into Materialize via the Fivetran SDK gRPC protocol.
The public API exports `MaterializeDestination` (the gRPC service implementation) and `DestinationConnectorServer` (the tonic server wrapper).
The crate is structured around a `destination` module containing DDL and DML handlers, with `crypto`, `utils`, `logging`, and `error` as supporting modules.
Key dependencies include `tonic`, `tokio-postgres`, `openssl`, `csv-async`, `async-compression`, `mz-pgrepr`, and `mz-sql-parser`; the binary target `mz-fivetran-destination` wires these together into a running gRPC server.
