FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /coinbase-connector ./coinbase-connector/cmd

FROM alpine:latest

COPY --from=builder /coinbase-connector /coinbase-connector

ENTRYPOINT ["/coinbase-connector"]
