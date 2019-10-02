FROM ubuntu:bionic

RUN apt-get update && apt-get install -qy build-essential unixodbc-dev

COPY . workdir/
RUN cd workdir && make

FROM ubuntu:bionic

RUN apt-get update && apt-get install -qy unixodbc

COPY --from=0 /workdir/build/chBenchmark /usr/local/bin/chBenchmark

ENTRYPOINT ["chBenchmark"]
