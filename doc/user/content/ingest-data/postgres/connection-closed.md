---
title: "Troubleshooting: Connection closed"
description: "How to troubleshoot and resolve the connection closed error with PostgreSQL sources in Materialize"
menu:
  main:
    parent: "pg-troubleshooting"
    name: "Connection closed"
    weight: 20
---

This guide helps you troubleshoot and resolve the "connection closed" error that
can occur with PostgreSQL sources in Materialize.

## What this error means

When you see an error like:

```nofmt
postgres: connection closed
```

This means the network connection between Materialize and your PostgreSQL
database was unexpectedly terminated. The connection that Materialize uses to
replicate data from PostgreSQL was closed, interrupting the replication process.

{{< note >}}
This error is known to occur during Materialize maintenance windows and can be
safely ignored if that is the case. Sources will automatically reconnect after
maintenance is complete.
{{</ note >}}

## Common causes

- **Network instability**: Intermittent network issues between Materialize and
  your PostgreSQL database can cause connections to drop.
- **Firewall or security group changes**: Changes to firewall rules, security
  groups, or network policies may block or terminate existing connections.
- **Database restarts or maintenance**: PostgreSQL server restarts, maintenance
  operations, or failovers can close active connections.
- **Connection timeouts**: Idle connection timeouts configured on PostgreSQL,
  load balancers, or network infrastructure may close connections that appear
  inactive.
- **Resource exhaustion**: PostgreSQL running out of available connections or
  memory may forcibly close connections.
- **Load balancer issues**: If connecting through a load balancer or proxy,
  connection pooling or timeout settings may cause unexpected disconnections.
- **Client certificate expiration**: If using SSL/TLS with client certificates,
  expired certificates can cause connection failures.

## Diagnosing the issue

### Check connection parameters

Verify your PostgreSQL connection configuration in Materialize:

```mzsql
SELECT name, connection_type
FROM mz_connections
WHERE type = 'postgres';
```

### Check for network connectivity

Test basic connectivity from Materialize to your PostgreSQL host. Verify that:

- The PostgreSQL host is reachable
- Firewall rules allow traffic
- DNS resolution is working correctly

### Check PostgreSQL logs

Review your PostgreSQL logs for connection-related messages:

```sql
SELECT * FROM pg_stat_activity
WHERE state_change < NOW() - INTERVAL '60 minutes';
```

Look for log entries indicating:

- Connection timeouts
- Authentication failures
- Resource exhaustion
- Server shutdowns or restarts

### Check PostgreSQL connection limits

Verify you haven't exceeded connection limits:

```sql
SELECT
  max_conn,
  used,
  res_for_super,
  max_conn - used - res_for_super AS remaining
FROM
  (SELECT count(*) AS used FROM pg_stat_activity) t1,
  (SELECT setting::int AS res_for_super FROM pg_settings WHERE name = 'superuser_reserved_connections') t2,
  (SELECT setting::int AS max_conn FROM pg_settings WHERE name = 'max_connections') t3;
```

### Check for idle connection timeouts

Check timeout settings that might close connections:

```sql
SHOW tcp_keepalives_idle;
SHOW tcp_keepalives_interval;
SHOW tcp_keepalives_count;
SHOW statement_timeout;
```

## Resolution

### Immediate fix: Materialize will automatically reconnect

In most cases, Materialize will automatically attempt to reconnect to PostgreSQL
when a connection is closed. Monitor your source to see if it recovers on its
own.

You can check the source status with:

```mzsql
SELECT *
FROM mz_internal.mz_source_statuses
WHERE id = 'your_source_id';
```

### Long-term fixes

**1. Increase connection keepalive settings**

Configure PostgreSQL to keep connections alive longer by adjusting TCP keepalive
settings in `postgresql.conf`:

```nofmt
tcp_keepalives_idle = 60
tcp_keepalives_interval = 10
tcp_keepalives_count = 5
```

Then reload the configuration:

```sql
SELECT pg_reload_conf();
```

**2. Configure connection timeout on network devices**

If using load balancers or proxies, ensure their idle timeout settings are
appropriate for long-lived replication connections:

- Set idle timeouts to at least 10-15 minutes
- Configure keepalive probes to detect stale connections

**3. Increase PostgreSQL connection limits**

If hitting connection limits, increase `max_connections` in `postgresql.conf`:

```nofmt
max_connections = 200
```

{{< note >}}
Increasing max_connections may require more shared memory. You may also need to
adjust `shared_buffers` and other memory settings.
{{</ note >}}

**4. Review and update SSL certificates**

If using SSL, verify certificate validity:

```sql
SELECT ssl,
       sslversion,
       sslcipher
FROM pg_stat_ssl
JOIN pg_stat_activity ON pg_stat_ssl.pid = pg_stat_activity.pid;
```

Ensure certificates are renewed before expiration.

**5. Implement network stability improvements**

- Use dedicated network paths for replication traffic
- Ensure adequate bandwidth between Materialize and PostgreSQL
- Minimize network hops and latency
- Consider using VPC peering or private connectivity options

## Prevention

**Best practices to avoid this error:**

- Configure appropriate TCP keepalive settings on PostgreSQL.
- Set reasonable connection timeouts on load balancers and proxies (10+ minutes
  for replication).
- Monitor network connectivity and latency between Materialize and PostgreSQL.
- Ensure PostgreSQL has adequate connection capacity (`max_connections`).
- Use SSL/TLS with valid, up-to-date certificates.
- Implement monitoring and alerting for connection failures.
- Schedule PostgreSQL maintenance during low-traffic periods.
- Use connection poolers like PgBouncer carefully (they may not work well with
  replication slots).

## Provider-specific considerations

### Amazon RDS

RDS may terminate idle connections after a period of inactivity. Ensure:

- Security groups allow traffic from Materialize
- Parameter groups have appropriate keepalive settings
- RDS maintenance windows are scheduled appropriately

### Google Cloud SQL

Cloud SQL has connection limits based on instance size:

- Monitor connection usage via Cloud SQL metrics
- Consider upgrading instance size if hitting limits
- Use private IP connectivity when possible for better stability

### Azure Database for PostgreSQL

Azure databases have connection limits and idle timeout policies:

- Review connection limits for your service tier
- Configure firewall rules to allow Materialize IP addresses
- Enable connection retry logic by ensuring Materialize can reconnect

### Self-managed PostgreSQL

You have full control over connection settings:

- Configure keepalive settings as recommended above
- Monitor system resources (memory, connections)
- Implement robust firewall rules that don't interfere with long-lived
  connections
- Consider using dedicated hardware or VMs for database hosting
