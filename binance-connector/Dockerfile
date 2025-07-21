FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /binance-connector ./binance-connector/cmd/main.go

# Final stage
FROM alpine:latest


COPY --from=builder /binance-connector /binance-connector

ENTRYPOINT ["/binance-connector"]
