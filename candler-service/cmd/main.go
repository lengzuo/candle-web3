package main

import (
	"context"
	"flag"
	"fmt"
	"hermeneutic/external/binance"
	"hermeneutic/internal/candles/aggregator"
	candlesgrpc "hermeneutic/internal/candles/transport/grpc"
	v1 "hermeneutic/pkg/proto/v1"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	// Configuration to set for intervals default 5 seconds
	interval := flag.Int("interval", 5, "The time interval in seconds for creating OHLC candles.")
	port := flag.Int("port", 8080, "The port for the grpc server to listen on.")
	flag.Parse()

	log.Info().
		Int("interval", *interval).
		Int("port", *port).
		Msg("starting candles-service")

	agg := aggregator.NewAggregator(time.Duration(*interval) * time.Second)
	defer agg.Stop()

	pairs := []string{"BTC-USDT", "ETH-USDT", "SOL-USDT"}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatal().Err(err).Msg("failed to listen for grpc server")
	}

	// Create a new grpc server
	s := grpc.NewServer()

	grpcServer := candlesgrpc.NewCandlesServer(agg)

	// Candles service register for grpc server
	v1.RegisterCandlesServiceServer(s, grpcServer)

	// Graceful Shutdown signal
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	binanceConnector := binance.NewConnector(agg, pairs)
	binanceConnector.Start(ctx)
	defer binanceConnector.Stop()

	// Start the grpc server in a separate goroutine
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
