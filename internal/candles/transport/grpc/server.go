package grpc

import (
	v1 "hermeneutic/pkg/proto/v1"

	"github.com/rs/zerolog/log"
)

type Broadcaster interface {
	RegisterClient(chan *v1.Candle)
	UnregisterClient(chan *v1.Candle)
}

type CandlesServer struct {
	v1.UnimplementedCandlesServiceServer
	broadcaster Broadcaster
}

func NewCandlesServer(broadcaster Broadcaster) *CandlesServer {
	return &CandlesServer{
		broadcaster: broadcaster,
	}
}

func (s *CandlesServer) SubscribeCandles(req *v1.SubscribeCandlesRequest, stream v1.CandlesService_SubscribeCandlesServer) error {
	subscribedPairs := make(map[string]struct{}, len(req.InstrumentPairs))
	for _, pair := range req.InstrumentPairs {
		subscribedPairs[pair] = struct{}{}
	}

	clientChan := make(chan *v1.Candle, 100)
	s.broadcaster.RegisterClient(clientChan)
	defer s.broadcaster.UnregisterClient(clientChan)

	ctx := stream.Context()
	log.Info().Msg("client subscribed for candles")

	for {
		select {
		case candle := <-clientChan:
			if _, ok := subscribedPairs[candle.InstrumentPair]; ok {
				if err := stream.Send(candle); err != nil {
					log.Err(err).Msg("failed to send stream")
					return err
				}
			}
		case <-ctx.Done():
			log.Info().Msg("client unsubscribed for candles")
			return ctx.Err()
		}
	}
}
