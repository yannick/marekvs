# marekvs — minimal OS-less image (design/08).
# Build context must be the PARENT directory (storage-engines/) so the
# ../ondadb path dependency resolves; the Justfile stages a clean context.
#
#   just docker-build     (docker)
#   just apple-build      (Apple container CLI)

# ---- build stage ----------------------------------------------------------
FROM rust:1-alpine AS build
RUN apk add --no-cache musl-dev build-base
WORKDIR /src
COPY ondadb/ ondadb/
COPY marekvs/ marekvs/
WORKDIR /src/marekvs
RUN cargo build --release -p marekvs-server -p marekvs-operator \
 && cp target/release/marekvs-server /marekvs \
 && cp target/release/marekvs-operator /marekvs-operator

# ---- runtime stage --------------------------------------------------------
FROM scratch
COPY --from=build /marekvs /marekvs
# Operator in the same image: deployments set command: ["/marekvs-operator"].
COPY --from=build /marekvs-operator /marekvs-operator
# No USER baked in: docker named volumes mount root-owned, and scratch has no
# way to chown. Kubernetes deployments set runAsUser/fsGroup via
# securityContext instead (design/07).
EXPOSE 6379 7373 7946/udp 9121
ENTRYPOINT ["/marekvs"]
