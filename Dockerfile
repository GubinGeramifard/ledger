# Build a static binary with the pinned Go toolchain.
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" -o /out/tally ./cmd/server

# Ship just the binary on a minimal, distroless base. The dashboard is embedded
# in the binary via go:embed, so this is the whole application.
FROM gcr.io/distroless/static-debian12
WORKDIR /data
ENV LEDGER_WAL=/data/ledger.wal
COPY --from=build /out/tally /tally
EXPOSE 8080
ENTRYPOINT ["/tally"]
