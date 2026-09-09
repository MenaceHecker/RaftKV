# Build the server and the benchmark tool.
#
# The build stage is pinned to the same Go version the module declares, so a
# container build and a local build produce the same binary rather than
# diverging quietly when a newer toolchain lands.
FROM golang:1.25-alpine AS build

WORKDIR /src

# Dependencies first, in their own layer: they change far less often than the
# source, so an edit to a .go file does not re-download the module cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO off gives a static binary, which is what lets the runtime stage be this
# small. The version stamp makes a running container traceable to a commit.
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/raftkv-server ./cmd/raftkv-server && \
    CGO_ENABLED=0 go build -trimpath \
      -o /out/raftkv-bench ./cmd/raftkv-bench

# Run the tests during the image build. An image that ships a binary whose
# tests were never run is an image nobody should deploy.
RUN go test ./...

FROM alpine:3.21

# wget is here for the compose healthcheck. Kubernetes probes the endpoint
# itself and does not need it, but a compose user without it gets a cluster
# that reports healthy the instant the process starts, which is exactly when
# it has not yet elected anything.
RUN apk add --no-cache wget ca-certificates && \
    adduser -D -u 10001 raftkv && \
    mkdir -p /data && chown raftkv:raftkv /data

COPY --from=build /out/raftkv-server /usr/local/bin/raftkv-server
COPY --from=build /out/raftkv-bench /usr/local/bin/raftkv-bench

# Never run as root. The process needs nothing but its data directory.
USER raftkv
VOLUME /data

# 9001 carries both Raft traffic and the client API; 9101 serves metrics and
# health on its own listener so monitoring survives a saturated data plane.
EXPOSE 9001 9101

ENTRYPOINT ["raftkv-server"]
