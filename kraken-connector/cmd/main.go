package main

import (
	"context"
	"hermeneutic/kraken-connector/external"
	"os"
	"os/signal"
	"strconv"
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

	numWorkersStr := os.Getenv("NUM_WORKERS")
	numWorkers, err := strconv.Atoi(numWorkersStr)
	if err != nil || numWorkers <= 0 {
		numWorkers = 100
	}

	pairs := []string{"BTC/USDT", "ETH/USDT", "SOL/USDT"}

	conn := external.NewConnector(producer, pairs, numWorkers)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn.Start(ctx)

	<-ctx.Done()

	log.Info().Msg("shutting down kraken connector...")
}
