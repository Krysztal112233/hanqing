# hanqing images, one per compose service, selected by build target:
#   target app — daemon + hanqing-cli (management CLI incl. `migrate`)
#   target db  — pgroonga (search) + pgmq (task queues) + pgvector (semantic search)
#
# rustls-only dependency tree; no system libraries needed beyond glibc.

FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --locked --release \
      -p hanqing -p hanqing-cli

FROM debian:bookworm-slim AS app
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --user-group hanqing

WORKDIR /app
COPY --from=build /src/target/release/hanqing /usr/local/bin/hanqing
COPY --from=build /src/target/release/hanqing-cli /usr/local/bin/hanqing-cli
# Config skeleton (max_connections only; credentials come from env).
COPY hanqing.toml /app/hanqing.toml

USER hanqing
CMD ["hanqing"]

# pgmq is a pure-SQL extension: installing it means copying the control file
# and the SQL install script into Postgres's extension directory (exactly
# what its `make install` does). Files are fetched at a pinned tag and
# verified by sha256. The core API hanqing uses (create/send/read/delete/
# set_vt/metrics) is stable across pgmq 1.x, so this does not need to match
# the production server's 1.11.2 exactly.
FROM groonga/pgroonga:4.1.0-debian-18 AS db

# pgvector from PGDG (the pgroonga image inherits the postgres image's apt repo).
RUN apt-get update \
 && apt-get install -y --no-install-recommends postgresql-18-pgvector \
 && rm -rf /var/lib/apt/lists/*

ARG PGMQ_VERSION=1.13.0
ADD https://github.com/pgmq/pgmq/raw/v${PGMQ_VERSION}/pgmq-extension/pgmq.control \
    /usr/share/postgresql/18/extension/pgmq.control
ADD https://github.com/pgmq/pgmq/raw/v${PGMQ_VERSION}/pgmq-extension/sql/pgmq.sql \
    /usr/share/postgresql/18/extension/pgmq--${PGMQ_VERSION}.sql

RUN echo "ba9075f2b06608e6ffef47eaa972cf72ef3309a2c759d3e18f0042b7f557700c  /usr/share/postgresql/18/extension/pgmq.control" | sha256sum -c - \
 && echo "bf7e66c75ed771df0c5f7cb2b9f0123ca77992daacd56e3911d1894a3efa99de  /usr/share/postgresql/18/extension/pgmq--${PGMQ_VERSION}.sql" | sha256sum -c - \
 && chmod 644 /usr/share/postgresql/18/extension/pgmq.control \
             /usr/share/postgresql/18/extension/pgmq--${PGMQ_VERSION}.sql
