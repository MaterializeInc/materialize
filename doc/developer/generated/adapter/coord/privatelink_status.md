---
source: src/adapter/src/coord/privatelink_status.rs
revision: e128c88627
---

# adapter::coord::privatelink_status

Polls the cloud provider for PrivateLink connection status and updates `mz_aws_privatelink_connection_status_history` when status changes are detected.
`PrivateLinkVpcEndpointMonitor` runs as a periodic task in the coordinator and maps cloud-API status responses to rows in the builtin table.
