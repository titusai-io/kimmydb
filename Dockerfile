# syntax=docker/dockerfile:1

# Where the kimmyd binary comes from. `build` (the default) compiles it in the
# stage below, which is what a laptop `docker build` does. CI and the release
# publish pass `prebuilt` to take a binary from `.prebuilt/kimmyd` in the
# context instead of compiling one a second time from cold inside BuildKit:
# CI the one its `build` job compiled for every other job, the release the
# one from the release archive for that architecture (ADR-107), so the image
# ships the file the Release page does.
ARG KIMMYD_SOURCE=build

# ---- build ----------------------------------------------------------------
FROM rust:1-slim-trixie AS build

WORKDIR /src

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# The build context has no .git, so the commit the binary reports would be
# "unknown". The release workflow passes the real one; a local `docker build`
# without the arg still works and honestly says so.
ARG GIT_COMMIT=
ENV KIMMY_BUILD_COMMIT=$GIT_COMMIT

# Cache mounts keep the cargo registry and target directory across builds, so a
# source-only change recompiles just the workspace crates. The binary is copied
# out inside the same RUN because cache mounts do not persist into the layer.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --bin kimmyd && \
    cp target/release/kimmyd /usr/local/bin/kimmyd

# ---- prebuilt -------------------------------------------------------------
# Only evaluated when KIMMYD_SOURCE=prebuilt, so the missing file is harmless
# for every other build.
FROM scratch AS prebuilt
COPY .prebuilt/kimmyd /usr/local/bin/kimmyd

# The stage the runtime copies from, resolved by the build arg above.
FROM ${KIMMYD_SOURCE} AS kimmyd-bin

# ---- runtime --------------------------------------------------------------
FROM debian:trixie-slim AS runtime

# In the image itself rather than only in workflow metadata, so an image from
# a laptop `docker build` still says where its source lives — and GHCR uses
# this label to link the package to the repository.
LABEL org.opencontainers.image.source="https://github.com/titusai-io/kimmydb"

# ca-certificates is needed by the remote embedding providers (OpenAI, Voyage,
# Ollama over TLS). The local ONNX provider needs no network at all.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /var/lib/kimmy --shell /usr/sbin/nologin kimmy

COPY --from=kimmyd-bin /usr/local/bin/kimmyd /usr/local/bin/kimmyd

# The data directory is a volume: it holds the redb file and, critically, the
# node identity. Losing it makes a restarted node a stranger to its own writes.
RUN mkdir -p /var/lib/kimmy && chown kimmy:kimmy /var/lib/kimmy
VOLUME ["/var/lib/kimmy"]

USER kimmy
WORKDIR /var/lib/kimmy

ENV KIMMY_DATA_DIR=/var/lib/kimmy \
    KIMMY_BIND=0.0.0.0:7878 \
    KIMMY_LOG_LEVEL=info

# 7878 = HTTP / WebSocket / MCP. 7900 = gossip (UDP and TCP).
EXPOSE 7878/tcp 7900/udp 7900/tcp

# No shell wrapper: kimmyd must receive SIGTERM directly as PID 1 so that
# `docker stop` and Kubernetes pod termination shut down gracefully.
ENTRYPOINT ["/usr/local/bin/kimmyd"]
CMD ["run"]
