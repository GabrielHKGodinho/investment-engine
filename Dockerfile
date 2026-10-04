# syntax=docker/dockerfile:1

# ---- Build stage: toolchain + source code, never shipped ----
FROM golang:1.25 AS build

WORKDIR /src

# Dependencies first: this step is only redone when go.mod/go.sum change.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# CGO is disabled on purpose: distroless/static has no libc, so the binaries
# must be fully static. The cache mounts keep Go's module and build caches
# across builds without writing them into any image layer.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/consumer ./cmd/consumer
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/priceservice ./cmd/priceservice

# ---- Runtime stages: one image per binary, selected with --target ----
# distroless/static ships CA certificates, tzdata and a nonroot user, and no
# shell. USER is numeric so platforms that enforce "run as non-root" can
# verify it without reading /etc/passwd.

FROM gcr.io/distroless/static-debian13:nonroot AS api
COPY --from=build /out/api /api
USER 65532:65532
EXPOSE 8080 9100
ENTRYPOINT ["/api"]

FROM gcr.io/distroless/static-debian13:nonroot AS consumer
COPY --from=build /out/consumer /consumer
USER 65532:65532
EXPOSE 9101
ENTRYPOINT ["/consumer"]

FROM gcr.io/distroless/static-debian13:nonroot AS priceservice
COPY --from=build /out/priceservice /priceservice
USER 65532:65532
EXPOSE 50051 9102
ENTRYPOINT ["/priceservice"]