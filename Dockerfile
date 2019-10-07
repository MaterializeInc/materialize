FROM ubuntu:bionic

RUN apt-get update && apt-get install -qy build-essential unixodbc-dev cmake

COPY . workdir/
RUN mkdir workdir/build && cd workdir/build && cmake -DCMAKE_BUILD_TYPE=Release .. && make

FROM ubuntu:bionic

RUN apt-get update && apt-get install -qy unixodbc

COPY --from=0 /workdir/build/chbenchmark /usr/local/bin/chBenchmark

ENTRYPOINT ["chBenchmark"]
