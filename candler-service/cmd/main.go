package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"hermeneutic/internal/candles/aggregator"
	"hermeneutic/internal/candles/broadcaster"
	candlesgrpc "hermeneutic/internal/candles/transport/grpc"
	"hermeneutic/internal/dto"
	v1 "hermeneutic/pkg/proto/v1"
	"hermeneutic/utils/async"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"

	"google.golang.org/grpc"
)

const topic = "trades"

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	interval := flag.Int("interval", 5, "The time interval in seconds for creating OHLC candles.")
	port := flag.Int("port", 8080, "The port for the grpc server to listen on.")
	flag.Parse()

	log.Info().
		Int("interval", *interval).
		Int("port", *port).
		Msg("starting candles-service")

	agg := aggregator.NewAggregator(time.Duration(*interval) * time.Second)
	defer agg.Stop()

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		log.Fatal().Msg("KAFKA_BROKERS environment variable not set")
	}

	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(kafkaBrokers, ","),
		Topic:   topic,
		GroupID: "candles-service",
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	async.Go(func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := consumer.ReadMessage(ctx)
				if err != nil {
					log.Error().Err(err).Msg("failed to read message from kafka")
					return
				}

				var trade dto.Trade
				if err := json.Unmarshal(msg.Value, &trade); err != nil {
					log.Warn().Err(err).Msg("failed to unmarshal trade")
					continue
				}

				agg.AddTrade(trade)
			}
		}
	})

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatal().Err(err).Msg("failed to listen for grpc server")
	}

	// Create a new grpc server
	s := grpc.NewServer()

	b := broadcaster.NewBroadcaster(agg.OutputChannel())
	grpcServer := candlesgrpc.NewCandlesServer(b)

	// Candles service register for grpc server
	v1.RegisterCandlesServiceServer(s, grpcServer)

	go func() {
		log.Info().Msgf("grpc server listening at %v", lis.Addr())
		if err := s.Serve(lis); err != nil {
			log.Fatal().Err(err).Msg("failed to serve grpc")
		}
	}()

	<-ctx.Done()

	log.Info().Msg("shutting down grpc server...")
	s.GracefulStop()
	log.Info().Msg("server gracefully stopped")
}
