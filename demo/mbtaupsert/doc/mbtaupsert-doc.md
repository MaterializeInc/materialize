# mbtaupsert binary documentation

Run the mbtaupsert binary with `-h`, `--help`, or no options at all to see usage.

## Kafka topic properties

The only manner in which properties of default topics created by this binary
differ from default Kafka topics is that `cleanup.policy` is set to `compact`.
If you want `cleanup.policy` to be `delete`, you should explicitly specify the
argument `--topic-property cleanup.policy=delete`.

## Experimental: BYO consistency support

The binary has some support for Materialize's experimental BYO consistency
feature, which you can turn on with the flag `--byo`. Specifically,
in addition to pushing the information from the MBTA streams into a Kafka topic
(let's call this the "data topic"), the binary can create a second consistency
topic that assigns a different, strictly monotonically increasing time stamp to
each record pushed into the data topic.

You can then create the Kafka source in Materialize with the option
`WITH(consistency = <consistency_topic_name>)`. The default consistency topic
name is `<name_of_data_topic>-consistency`.

Note that the support is quite primitive at the moment, so:
1. If you are using BYO consistency, make sure that the name of the
   consistency topic is not the name of a preexisting topic with non-BYO
   consistency data in it. You can specify a consistency topic name with
   `-c <new_topic_name>`.
2. Joins between different streams or with other types of sources cannot be
   done. If you try, wonky results may result.
3. Due to lack of timestamp coordination between different instances of the
   binary, each binary should be pushing MBTA data to separate data topics,
   though multiple data topics can share the same consistency topic.
