FROM ubuntu:disco

RUN apt-get update && apt-get install -qy build-essential unixodbc-dev cmake libpqxx-dev libconfig-dev

COPY . workdir/
RUN mkdir workdir/build && cd workdir/build && cmake -DCMAKE_BUILD_TYPE=Release .. && make

FROM ubuntu:disco

RUN apt-get update && apt-get install -qy curl gnupg && curl 'https://packages.confluent.io/deb/5.3/archive.key' | apt-key add - && echo 'deb [arch=amd64] https://packages.confluent.io/deb/5.3 stable main' >> /etc/apt/sources.list && apt-get update && apt-get install -qy unixodbc libpqxx-6.2 locales openjdk-8-jre jq confluent-schema-registry libconfig9 && locale-gen en_US.UTF-8

COPY --from=0 /workdir/build/chbenchmark /usr/local/bin/chBenchmark
COPY ./flush-tables /usr/local/bin/flush-tables

ENTRYPOINT ["chBenchmark"]
