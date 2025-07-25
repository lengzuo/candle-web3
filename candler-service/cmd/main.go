package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"hermeneutic/candler-service/internal/aggregator"
	"hermeneutic/candler-service/internal/broadcaster"
	candlesgrpc "hermeneutic/candler-service/internal/grpc"
	"hermeneutic/candler-service/internal/publisher"
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

	agg := aggregator.NewAggregator(time.Duration(*interval)*time.Second, 1*time.Second)
	defer agg.Stop()

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		log.Fatal().Msg("KAFKA_BROKERS environment variable not set")
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		log.Fatal().Msg("REDIS_ADDR environment variable not set")
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

	pub := publisher.NewPublisher(redisAddr, agg.OutputChannel())
	defer pub.Stop()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatal().Err(err).Msg("failed to listen for grpc server")
	}

	s := grpc.NewServer()
	b := broadcaster.NewBroadcaster(redisAddr)
	grpcServer := candlesgrpc.NewCandlesServer(b)
	v1.RegisterCandlesServiceServer(s, grpcServer)

	go func() {
		log.Info().Msgf("grpc server listening at %v", lis.Addr())
		if err := s.Serve(lis); err != nil {
			log.Fatal().Err(err).Msg("failed to serve grpc")
		}
	}()

	<-ctx.Done()

	log.Info().Msg("shutting down grpc server...")
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()

	// Attempt graceful shutdown
	go func() {
		s.GracefulStop()
		log.Info().Msg("server gracefully stopped")
		cancelShutdown() // Signal that graceful stop is complete
	}()

	// Wait for either graceful stop to complete or timeout
	<-shutdownCtx.Done()

	// If the context was cancelled by timeout, it means graceful stop didn't finish in time
	if shutdownCtx.Err() == context.DeadlineExceeded {
		log.Warn().Msg("graceful shutdown timed out, forcing server stop...")
		s.Stop() // Force stop if graceful shutdown takes too long
		log.Info().Msg("server forcefully stopped")
	}
}
