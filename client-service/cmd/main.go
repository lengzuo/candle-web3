package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	v1 "hermeneutic/pkg/proto/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	serverAddr := flag.String("server", "candles-service:8080", "The address of the candles-service gRPC server.")
	pairsStr := flag.String("pairs", "btcusdt,ethusdt,solusdt", "A comma-separated list of instrument pairs to subscribe to.")
	flag.Parse()

	pairs := strings.Split(*pairsStr, ",")
	if len(pairs) == 0 {
		log.Fatal("please provide at least one instrument pair to subscribe to.")
	}

	log.Printf("attempting to connect to server at %s with subscribed pair of: %s", *serverAddr, *pairsStr)

	conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to grpc server: %v", err)
	}
	defer conn.Close()

	client := v1.NewCandlesServiceClient(conn)
	log.Println("successfully connected to grpc server.")

	// Subscription pair
	req := &v1.SubscribeCandlesRequest{
		InstrumentPairs: pairs,
	}

	// Create a context for the stream that can be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("received shutdown signal, cancelling context...")
		cancel()
	}()

	stream, err := client.SubscribeCandles(ctx, req)
	if err != nil {
		log.Fatalf("failed to subscribe to candles: %v", err)
	}

	log.Printf("subscribed to pairs: %v. waiting for candles...", pairs)

	for {
		select {
		case <-ctx.Done():
			log.Println("context cancelled, stopping candle reception.")
			return
		default:
			candle, err := stream.Recv()
			if err != nil {
				log.Printf("error receiving candle from stream: %v", err)
				return // Exit loop on stream error or context cancellation
			}

			// Print the received candle to stdout
			fmt.Printf("🕯️  [%s] Pair: %s | Open: %s | High: %s | Low: %s | Close: %s | Volume: %s\n",
				candle.Timestamp.AsTime().Format(time.RFC3339),
				candle.InstrumentPair,
				candle.Open,
				candle.High,
				candle.Low,
				candle.Close,
				candle.Volume,
			)
		}
	}
}
