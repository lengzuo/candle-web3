FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /kraken-connector ./kraken-connector/cmd/main.go

# final stage
FROM alpine:latest


COPY --from=builder /kraken-connector /kraken-connector

ENTRYPOINT ["/kraken-connector"]
