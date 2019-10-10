FROM ubuntu:disco

RUN apt-get update && apt-get install -qy build-essential unixodbc-dev cmake libpqxx-dev

COPY . workdir/
RUN mkdir workdir/build && cd workdir/build && cmake -DCMAKE_BUILD_TYPE=Release .. && make

FROM ubuntu:disco

RUN apt-get update && apt-get install -qy unixodbc libpqxx-6.2

COPY --from=0 /workdir/build/chbenchmark /usr/local/bin/chBenchmark

ENTRYPOINT ["chBenchmark"]
