package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	v1 "hermeneutic/pkg/proto/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	serverAddr := flag.String("server", "candles-service:8080", "The address of the candles-service gRPC server.")
	pairsStr := flag.String("pairs", "btcusdt,ethusdt", "A comma-separated list of instrument pairs to subscribe to.")
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

	// Create a context for the stream for 10 mins for demo
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	stream, err := client.SubscribeCandles(ctx, req)
	if err != nil {
		log.Fatalf("failed to subscribe to candles: %v", err)
	}

	log.Printf("subscribed to pairs: %v. waiting for candles...", pairs)

	for {
		candle, err := stream.Recv()
		if err != nil {
			log.Fatalf("error receiving candle from stream: %v", err)
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
