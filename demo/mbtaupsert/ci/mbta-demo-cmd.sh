#!/usr/bin/env bash

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
#
# mbta-demo-cmd.sh — runs the mbta demo

mbtaupsert=$(whereis mbtaupsert)

if [ -z "$mbtaupsert" ]; then
  mbtaupsert=target/release/mbtaupsert
fi;

function archive() {
  current_datetime=$(date +'%F-%H%M')
  archive_name="workspace-$current_datetime"
  folder_name="archive/$archive_name"
  mkdir "$folder_name"
  cp workspace/mbta*log "$folder_name"
  cp -r workspace/MBTA_GTFS "$folder_name"

  if [[ "$(ls -A "$folder_name")" ]]; then
    cd "$folder_name" || exit
    tar -czf "../$archive_name.tar.gz" ./*
    cd ../../
  fi
  rm -rf "$folder_name"

  echo "Created archive at $folder_name"
}

function cleanup_kafka_topics() {
  config_file=$1
  kafka_addr=$2

  if [ -z "$kafka_addr" ]; then
    kafka_addr=localhost:9092
  fi
  # derive list of topics to delete
  awk 'BEGIN { FS = "," } ; { if ($1=="") { if($3=="") {print "mbta-" $2} else {print "mbta-" $2 "-" $3 "-" $4} } else { print $1 } }' "$config_file" > workspace/topics.log
  sort workspace/topics.log | uniq > workspace/unique-topics.log

  while read -r line
  do
    echo "Deleting kafka topic $line"
    kafka-topics --bootstrap-server $kafka_addr --delete --topic "$line"
  done < workspace/unique-topics.log

  # cleanup temp files
  rm workspace/topics.log
  rm workspace/unique-topics.log
}

function cleanup_http_streams() {
  sort workspace/curl-pid.log -n -r > workspace/sorted-curl-pid.log

  # killing curl threads must come after killing the threads that repeatedly
  # spawn curl threads
  current_stream=-1
  IFS=','
  while read -r stream_num _ pid
  do
    if [ $current_stream != "$stream_num" ]; then
      echo "Killing process at pid $pid"
      kill -9 "$pid"
      current_stream=$stream_num
    fi
  done < workspace/sorted-curl-pid.log
}

function cleanup_stream_to_kafka() {
  if [ -f workspace/steady-pid.log ]; then
    while read -r line
    do
      echo "Killing process at pid $line"
      kill -9 "$line"
    done < workspace/steady-pid.log
  fi
}

function cleanup_log_files() {
  # clean workspace
  rm -f workspace/*.log
  rm -f workspace/current_time
}

# create an indefinitely running connection to an mbta event stream
function create_connection() {
  line_num=$1
  api_key=$2
  filename=$3
  stream_type=$4
  filter_type=$5
  filter_on=$6

  echo "Cleaning up temp file $filename"
  rm -f "$filename"

  url="https://api-v3.mbta.com/$stream_type"
  if [ -n "$filter_type" ]; then
    url="$url/?filter\\[$filter_type\\]=$filter_on"
  fi

  curl_num=0
  while true; do
    curl -sN -H 'accept: text/event-stream' -H "x-api-key:$api_key" \
      "$url" >> "$filename" &
    mypid=$!
    echo "$line_num,$curl_num,$!" >> workspace/curl-pid.log
    wait $mypid
    (( curl_num+=1 ))
  done
}

# Create a kafka topic out of each stream
function start_stream_convert() {
  api_key=$1
  partitions=$2
  kafka_addr=$3
  topic_name=$4
  filename=$5
  heartbeat_time=$6

  options=(--kafka-addr "$kafka_addr" -f "$filename" -t "$topic_name" -p "$partitions" )
  if [ -n "$heartbeat_time" ]; then
    options+=( --heartbeat "$heartbeat_time" )
  fi

  if [ "$api_key" == "None" ]; then
    options+=( --exit-at-end )
  fi
  $mbtaupsert "${options[@]}" &

  echo "$!" >> workspace/steady-pid.log
}

# Read the config file and pass arguments from each line to start_stream
function start_streams_from_config_file() {
  config_file=$1
  api_key=$2
  kafka_addr=$3

  if [ -z "$kafka_addr" ]; then
    kafka_addr=localhost:9092
  fi

  while true ; do date +%s >> workspace/current_time ; sleep 1; done &
  echo "$!" >> workspace/steady-pid.log

  IFS=','
  line_num=0
  while read -r topic_name stream_type filter_type filter_on partitions heartbeat_time
  do
    if [ -z "$topic_name" ]; then
      if [ -n "$filter_type" ]; then
        topic_name="mbta-$stream_type-$filter_type-$filter_on"
      else
        topic_name="mbta-$stream_type"
      fi
    fi

    if [ -z "$partitions" ]; then
      partitions=1
    fi

    file_id="mbta-$stream_type"
    if [ -n "$filter_type" ]; then
      file_id="$file_id-$filter_type-$filter_on"
    fi

    filename="workspace/$file_id.log"

    if [ "$api_key" != "None" ] ; then
      create_connection $line_num "$api_key" "$filename" "$stream_type" "$filter_type" "$filter_on" &
      echo "$!" >> workspace/steady-pid.log
      #The first thing the stream sends is a snapshot of the current state of the
      #system. If we start reading from the stream too quickly, the snapshot can be
      #incomplete.
      sleep 5
    fi

    start_stream_convert "$api_key" $partitions $kafka_addr "$topic_name" "$filename" "$heartbeat_time"
    (( line_num+=1 ))
  done < "$config_file"
}

function pause() {
  archive;
  cleanup_stream_to_kafka;
  cleanup_http_streams;
  cleanup_log_files;
}

function refresh_metadata() {
  if [[ -f "workspace/last-metadata" ]]; then
    eval "$(tr -d '\r\n' < workspace/last-metadata)"
  else
    curl -s https://cdn.mbta.com/MBTA_GTFS.zip -J -L -o workspace/MBTA_GTFS.zip
  fi

  if [[ -s workspace/MBTA_GTFS.zip ]]; then
    echo -n 'curl -H "If-Modified-Since: ' > workspace/last-metadata
    curl -sI https://cdn.mbta.com/MBTA_GTFS.zip | grep last-modified \
     | cut -c 16- | tr -d '\r\n'>> workspace/last-metadata
    echo -n  '" https://cdn.mbta.com/MBTA_GTFS.zip -J -L -o workspace/MBTA_GTFS.zip' \
       >> workspace/last-metadata
    rm -rf workspace/MBTA_GTFS
    mkdir workspace/MBTA_GTFS
    unzip workspace/MBTA_GTFS.zip -d workspace/MBTA_GTFS
  fi

  rm workspace/MBTA_GTFS.zip
}

function unpack_archive() {
  # copy archive to workspace and untar
  archive_path=$1
  cp "$archive_path" workspace
  (
    cd workspace || exit
    tar -xzf "$(basename "$archive_path")"
  )
}

function wait_for_stream_conversion() {
  if [ -f workspace/steady-pid.log ]; then
    while read -r line
    do
      wait "$line"
    done < workspace/steady-pid.log
  fi
}

### start of script

case "$1" in
  start)
    if [[ $# -lt 3 ]]; then
      echo "usage: $0 start <config-file> <api-key> [kafka-addr]"
      exit 1
    fi
    refresh_metadata;
    start_streams_from_config_file "$2" "$3" "$4";
    ;;
  start_docker)
    if [[ $# -lt 3 ]]; then
      echo "usage: $0 start_docker <config-file> [kafka-addr] [api-key]"
      exit 1
    fi
    if [[ "$3" == *":"* ]]; then
      kafka_addr=$3
      api_key=$4
    else
      api_key=$3
    fi
    if [[ -z "$api_key" ]]; then
      api_key=$(cat /run/secrets/mbta_api_key)
    fi
    refresh_metadata;
    start_streams_from_config_file "$2" "$api_key" "$kafka_addr";
    trap "pause; exit " SIGTERM
    while : ; do wait ; done
    ;;
  pause)
    pause;
    ;;
  replay)
    if [[ $# -lt 3 ]]; then
      echo "usage: $0 replay <config-file> <archive> [kafka-addr]"
      echo ""
      exit 1
    fi
    unpack_archive "$3"
    start_streams_from_config_file "$2" None "$4";
    wait_for_stream_conversion;
    ;;
  archive)
    archive;
    ;;
  purge)
    if [[ $# -lt 2 ]]; then
      echo "usage: $0 purge <config-file> [kafka-addr]"
      echo ""
      exit 1
    fi
    pause;
    cleanup_kafka_topics "$2" "$3"
    ;;
  purge_topics)
    if [[ $# -lt 2 ]]; then
      echo "usage: $0 purge_topics <config-file> [kafka-addr]"
      echo ""
      exit 1
    fi
    cleanup_kafka_topics "$2" "$3"
    ;;
  *)
    echo "Usage: $0 COMMAND [arguments]

Set up MBTA streams and convert them into Kafka topics to ingest using Materialize

Commands:
start        Starts an indefinitely long run of the demo in the
             background. Call this script again with the command
             'pause' or 'purge' to stop the demo.
archive      Creates an archive of the files containing data downloaded
             from the MBTA streams.
replay       Replay data from an archive into a kafka topic. This script
             will remain in the foreground until the replay is complete.
pause        Archives the files, halts the http streaming threads, and
             halts the processes
purge_topics Deletes just the kafka topics.
purge        Pause demo then purges the kafka topics.
start_docker Starts an indefinitely long run of the demo in the
             foreground. The tasks in 'pause' will run automatically
             upon receiving a SIGTERM."
    exit 1
esac
