# Emulator

The emulator is a version of materialize used primarily for local testing. User documentation can be found [here](https://materialize.com/docs/get-started/install-materialize-emulator/).

## How to profile the emulator

Run the emulator using the following command to allow for local access to the heap dump endpoints for usage with the [debug tool](https://materialize.com/docs/integrations/mz-debug/).

```bash
  docker run -d \
        --name materialized \
        -p 6874:6874 \
        -p 6875:6875 \
        -p 6877:6877 \
        -p 6878:6878 \
        -e "MZ_NO_TELEMETRY=0" \
        materialize/materialized:latest \
        --cluster-replica-sizes='{"3xsmall": {"workers": 1, "scale": 1, "credits_per_hour": "1", "memory_limit": "12G"}, "2xsmall": {"workers": 1, "scale": 1, "credits_per_hour": "1", "memory_limit": "12G"}, "25cc": {"workers": 1, "scale": 1, "credits_per_hour": "1", "memory_limit": "24G"}, "50cc": {"workers": 1, "scale": 1, "credits_per_hour": "1", "memory_limit": "48G"}}' \
        --bootstrap-default-cluster-replica-size=3xsmall \
        --bootstrap-builtin-system-cluster-replica-size=3xsmall \
        --bootstrap-builtin-catalog-server-cluster-replica-size=3xsmall \
        --bootstrap-builtin-support-cluster-replica-size=3xsmall \
        --bootstrap-builtin-probe-cluster-replica-size=3xsmall \
        --availability-zone=test1 \
        --availability-zone=test2 \
        --aws-account-id=123456789000 \
        --aws-external-id-prefix=00000000-00000000-00000000-00000000 \
        --aws-connection-role-arn=arn:aws:iam::123456789000:role/MaterializeConnection \
        --system-parameter-default=max_clusters=100 \
        --system-parameter-default=max_sources=10000 \
        --system-parameter-default=max_objects_per_schema=10000 \
        --orchestrator-process-tcp-proxy-listen-addr=0.0.0.0
```

Navigate to `http://localhost:6878` to view debug information for environtmentd.

Note that when using the `latest` tag, you might have to pull the latest version manually if you had already used it previously:

```bash
docker pull --no-cache materialize/materialized:latest
```

**Linux Only**

To profile using the [parka agent](https://github.com/parca-dev/parca-agent) you must be running on Linux as it requires sharing a variety of directories with the emulator container.

```bash
  docker run -d \
      -p 7071:7071 \
      --privileged \
      --pid host \
      -v /sys/fs/bpf:/sys/fs/bpf \
      -v /run:/run \
      -v /boot:/boot \
      -v /lib/modules:/lib/modules \
      -v /sys/kernel/debug:/sys/kernel/debug \
      -v /sys/fs/cgroup:/sys/fs/cgroup \
      -v /var/run/dbus/system_bus_socket:/var/run/dbus/system_bus_socket \
      ghcr.io/parca-dev/parca-agent:v0.36.0 \
      --remote-store-address=grpc.polarsignals.com:443 \
      --remote-store-bearer-token=<redacted>
```
