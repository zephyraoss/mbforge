FROM docker.io/library/golang:1.24-bookworm AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 \
    go build -ldflags="-s -w" -o /out/mbforge ./cmd/mbforge

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends xz-utils curl ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/mbforge /usr/local/bin/mbforge
ENTRYPOINT ["mbforge"]
