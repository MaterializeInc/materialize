FROM ubuntu:bionic

RUN apt-get update && apt-get install -qy build-essential unixodbc-dev

COPY . build/

RUN cd build && make

FROM ubuntu:bionic

RUN apt-get update && apt-get install -qy unixodbc

COPY --from=0 /build/chBenchmark /usr/local/bin/chBenchmark
