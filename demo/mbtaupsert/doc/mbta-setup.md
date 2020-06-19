# Setting up the MBTA Stream

## Notes

The Boston public transit system goes to sleep at around midnight local time and
generally does not wake up until 5-6 am, so there may be no data in the stream
when the system has closed for the night. 

To make sure that the demo works properly, avoid changing things in 
[workspace](../workspace). 

All the setup assumes you are running commands from [mbtaupsert directory](../)
(<materialize root directory>/demo/mbtaupsert).

## One time setup

Get an API key from https://api-v3.mbta.com/ . This will enable you to send 1000 requests per minute to MBTA website.

## Automatic setup (Docker)

To automatically start the demo using Docker, run 
`API_KEY=<insert_api_key_here> ../../bin/mzconduct run mbtaupsert -w start-live-data`. 

By default, the streams that get fired up correspond to the ones in
[demo/all-frequent-routes-config-weekend](../demo/all-frequent-routes-config-weekend),
but if you want to use a different set of streams, [create an alternate config
file](../demo/README.md) and then set the environment variable
`CONFIG_FILE_PATH=/workdir/configs/<name_of_config_file>`

If you look in [mzcompose.yml](../mzcompose.yml), there are some other
environment variables that can be set to customize the setup.

To tear down the demo, run `../../bin/mzconduct down mbtaupsert`.

### Automatic Archiving

The Docker container comes with the ability to automatically archive the files
containing the data pulled from the MBTA streams at shutdown in case you want to
replay the data or give the data to someone else so they can replay the stream.
By default, Docker waits 10 seconds for the container to stop before killing it.
If you need more time to ensure that archiving completes, find the Docker
container corresponding to the demo with `docker ps | mbtaupsert_mbta-demo`.
Then run `docker stop <container_id> -t 30`. The `30` can be replaced with
however many seconds you think it will take to archive all the data. You can
tear down the rest of the containers using the command above.

Archives can be found at `workspace-<current_date_and_time>.tar.gz` file 
[in the `archive` directory](../archive).

To replay an archive, run 
`API_KEY=<insert_api_key_here> ARCHIVE_PATH=<insert_path_archive> ../../bin/mzconduct run mbtaupsert -w replay`. Do not uncompress the archive.

## Automatic setup (Bash)

To start and tear down a bunch of streams at once, create a config file like 
[this one](/demo/predictions-config).

Then run `ci/mbta-demo-cmd.sh start <path_to_config_file> <api_key> <kafka_address>`.
This should start up live kafka streams for every stream you specified in the
config file.

To tear down all the streams at once, run 
`ci/mbta-demo-cmd.sh purge <path_to_config_file> <kafka_address>`.
The data downloaded from the MBTA streams will be automatically archived.
Replace `purge` with `pause` if you don't want to delete the kafka topics created.

`<kafka_address>` is optional and defaults to `localhost:9092`.

Run `ci/mbta-demo-cmd.sh` without any options to see all the available commands.
Run `ci/mbta-demo-cmd.sh COMMAND` to see the usage for that command.

*Note 1*: To ensure a clean teardown, it is recommended that you wait for startup to
complete before running the teardown script.

*Note 2*: The `workspace` stores all the temporary files used in the setup
and teardown. Notably, the `workspace/steady-pid.log` and `workspace/curl-pid.log` store
the process ids of the background processes keeping all your streams alive. If you lose one
of these two files somehow and want to tear down the processes, use:
* `pkill mbtaupsert` to kill all the code pushing streams into Kafka.
* `ps -f|grep mbta-demo-cmd` to find the pids for the connection maintenance threads.
* `pkill curl` to kill all the curl commands downloading the streams. Note that
   you should kill the connection maintenance threads before killing the curl commands because
   the connection maintenance threads will spin up a new curl command if the curl command goes
   down. 
* `ps -f|grep "sleep 1"` to find the thread that prints the current time every
  second. Note that you want to kill the parent pid as opposed to the pid itself.

*Note 3*: Kafka topic deletion doesn't seem to always delete the metadata
   associated with a topic. If you want to recreate a topic with say, a
   different number of partitions, you should rename it in the config file. 

## Manual setup (Unix)

Download the stream metadata from https://www.mbta.com/developers/gtfs. The MBTA
recommends re-downloading metadata on a daily basis.

To manually connect to a single MBTA stream and load it into Kafka:

1. Open a connection using `curl` and write it into file.

  ```
  curl -sN -H "accept: text/event-stream" -H "x-api-key:INSERT_API_KEY_HERE" \
    "https://api-v3.mbta.com/INSERT_DESIRED_API" > name-of-file.log
  ```

  List of the live streams is here: https://api-v3.mbta.com/docs/swagger/index.html

  Example:

  The following command opens a connection to a stream of predicted arrival and departure times
  of Red Line trains allow all Red Line stops:

  ```
  curl -sN -H "accept: text/event-stream" -H "x-api-key:INSERT_API_KEY_HERE" \
    "https://api-v3.mbta.com/predictions/?filter\\[route\\]=Red" > red-stream.log
  ```

2. Convert the file into a key-value kafka topic like so: 

  ```
  cargo run -- -f path_to_stream_file.log -t topic_name
  ``` 

  See the [mbtaupsert package documentation](../mbtaupsert-doc.md) for more
  details on other options you can run with.

  You can use `kafka-console-consumer` to verify the correctness of your stream like this:

  ```
  kafka-console-consumer --bootstrap-server <kafka_address> --topic <topic_name> \
     --property print.key=true --from-beginning
  ```

  The code does not auto-delete old kafka topics. To delete a kafka topic, enter:

  ```
  kafka-topics --zookeeper <zookeeper_address> --delete --topic <topic_name>
  ```

3. Turn on materialize and create the sources and views corresponding to the kafka topics.

  Check out the reference for creating a view to parse each type of stream [here](../mbta-reference.md). 
