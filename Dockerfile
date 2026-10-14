# hanqing all-in-one image: daemon + hanqing-cli + migration.
# rustls-only dependency tree; no system libraries needed beyond glibc.

FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --locked --release \
      -p hanqing -p hanqing-cli -p migration

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --user-group hanqing

WORKDIR /app
COPY --from=build /src/target/release/hanqing /usr/local/bin/hanqing
COPY --from=build /src/target/release/hanqing-cli /usr/local/bin/hanqing-cli
COPY --from=build /src/target/release/migration /usr/local/bin/migration
# Config skeleton (max_connections only; credentials come from env).
COPY hanqing.toml /app/hanqing.toml

USER hanqing
CMD ["hanqing"]
