package main

import (
	"context"
	binanceconnector "hermeneutic/binance-connector"
	"hermeneutic/binance-connector/external"
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

const topic = "trades"

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	if os.Getenv("ENV") == "production" {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
		log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
	}
	_ = godotenv.Load()

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		log.Fatal().Msg("KAFKA_BROKERS environment variable not set")
	}

	numWorkersStr := os.Getenv("NUM_WORKERS")
	numWorkers, err := strconv.Atoi(numWorkersStr)
	if err != nil || numWorkers <= 0 {
		numWorkers = 500
	}

	producer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: strings.Split(kafkaBrokers, ","),
		Topic:   topic,
	})
	defer producer.Close()

	pairs := []string{binanceconnector.BTC_USDT.String(), binanceconnector.ETH_USDT.String(), binanceconnector.SOL_USDT.String()}

	conn := external.NewConnector(producer, pairs, numWorkers)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn.Start(ctx)

	<-ctx.Done()

	log.Info().Msg("shutting down binance connector...")
}
