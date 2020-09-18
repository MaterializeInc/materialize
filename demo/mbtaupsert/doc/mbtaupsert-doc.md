# mbtaupsert binary documentation

Run the mbtaupsert binary with `-h`, `--help`, or no options at all to see usage.

The binary supports using either:
1. a demo config file [see here](../demo/README.md), which is passed in with the
   `-c` command line argument. The config file allows the binary to convert multiple
   logs containing MBTA stream data into Kafka streams. The config file assumes
   the logs are in `../workspace` and follow the same naming convention
2. Manual specification using command line arguments of the configuration for a
   single log-to-Kafka topic conversion. At the bare minimum, you need to
   specify the location of the log to tail (`-f`) and the target topic name (`-t`).

All topics created by a single mbtaupsert binary run have the same Kafka topic properties.

By default, the binary will tail logs indefinitely. To have the binary stop upon
reaching the end of all the logs (for example, if you are replaying data), use
the command line argument `--exit-at-end`.

## Kafka topic creation

The only manner in which properties of default topics created by this binary
differ from default Kafka topics is that `cleanup.policy` is set to `compact`.
If you want `cleanup.policy` to be `delete`, you should explicitly specify the
argument `--topic-property cleanup.policy=delete`.

It is not possible to use the config file if you want different topics to have
different topic properties or replications.

If you want to manually create the Kafka topic yourself, you can disable Kafka
topic creation using command line argument `-d`. Note that if a topic already
exists, and you create it again using the mbtaupsert binary, it will be a no-op
unless you try to recreate it with different properties.
