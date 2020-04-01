FROM python:3.7-slim-buster

COPY . /app
RUN apt-get update -o Acquire::Languages=none \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install \
            -qy --no-install-recommends \
            -o Dpkg::Options::=--force-unsafe-io \
                libpq5 \
                libpq-dev \
                build-essential \
    && cd /app \ && pip install -e . \
    # clean up build dependencies
    && apt-get remove --purge -qy \
        libpq-dev \
        build-essential \
    && apt-get autoremove -qy \
    # the apt upstream package index gets stale very quickly
    && rm -rf \
        /var/cache/apt/archives \
        /var/lib/apt/lists/* \
        /root/.cache/pip/ \
    ;

CMD ["mzcli", "host=materialized port=6875 sslmode=disable"]
