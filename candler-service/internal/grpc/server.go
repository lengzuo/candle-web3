package grpc

import (
	"hermeneutic/candler-service/internal/broadcaster"
	v1 "hermeneutic/pkg/proto/v1"

	"github.com/rs/zerolog/log"
)

type CandlesServer struct {
	v1.UnimplementedCandlesServiceServer
	broadcaster *broadcaster.Broadcaster
}

func NewCandlesServer(b *broadcaster.Broadcaster) *CandlesServer {
	return &CandlesServer{broadcaster: b}
}

func (s *CandlesServer) SubscribeCandles(req *v1.SubscribeCandlesRequest, stream v1.CandlesService_SubscribeCandlesServer) error {
	log.Info().Strs("pairs", req.InstrumentPairs).Msg("client subscribed")
	candleChan := make(chan *v1.Candle, 100)

	for _, pair := range req.InstrumentPairs {
		s.broadcaster.Subscribe(pair, candleChan)
	}

	defer func() {
		for _, pair := range req.InstrumentPairs {
			s.broadcaster.Unsubscribe(pair, candleChan)
		}
		close(candleChan)
		log.Info().Strs("pairs", req.InstrumentPairs).Msg("client unsubscribed")
	}()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case candle := <-candleChan:
			if err := stream.Send(candle); err != nil {
				log.Error().Err(err).Msg("failed to send candle to client")
				return err
			}
		}
	}
}
