FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /app/candles-service ./candler-service/cmd/main.go

FROM alpine:latest

WORKDIR /root/

COPY --from=builder /app/candles-service .

EXPOSE 8080

CMD ["./candles-service"]
