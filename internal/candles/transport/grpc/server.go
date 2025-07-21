package grpc

import (
	v1 "hermeneutic/pkg/proto/v1"

	"github.com/rs/zerolog/log"
)

type Aggregator interface {
	CandleChannel() <-chan *v1.Candle
}

type CandlesServer struct {
	v1.UnimplementedCandlesServiceServer
	aggregator Aggregator
}

func NewCandlesServer(aggregator Aggregator) *CandlesServer {
	return &CandlesServer{
		aggregator: aggregator,
	}
}

func (s *CandlesServer) SubscribeCandles(req *v1.SubscribeCandlesRequest, stream v1.CandlesService_SubscribeCandlesServer) error {
	subscribedPairs := make(map[string]struct{}, len(req.InstrumentPairs))
	for _, pair := range req.InstrumentPairs {
		subscribedPairs[pair] = struct{}{}
	}

	candleChan := s.aggregator.CandleChannel()
	ctx := stream.Context()

	for {
		select {
		case candle := <-candleChan:
			if _, ok := subscribedPairs[candle.InstrumentPair]; ok {
				if err := stream.Send(candle); err != nil {
					log.Err(err).Msgf("failed in sending stream")
					return err
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
