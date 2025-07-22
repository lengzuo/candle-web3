package main

import (
	"context"
	"hermeneutic/coinbase-connector/external"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	_ = godotenv.Load()

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		log.Fatal().Msg("KAFKA_BROKERS environment variable not set")
	}

	producer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: strings.Split(kafkaBrokers, ","),
		Topic:   "trades",
	})
	defer producer.Close()

	pairs := []string{"BTC-USDT", "ETH-USDT", "SOL-USDT"}

	conn := external.NewConnector(producer, pairs)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn.Start(ctx)

	<-ctx.Done()

	log.Info().Msg("shutting down coinbase connector...")
}
