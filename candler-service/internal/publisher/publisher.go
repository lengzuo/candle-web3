package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	v1 "hermeneutic/pkg/proto/v1"
	"hermeneutic/utils/async"

	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
)

type Publisher struct {
	redisClient *redis.Client
	inputChan   <-chan *v1.Candle
	stopChan    chan struct{}
}

func NewPublisher(redisAddr string, inputChan <-chan *v1.Candle) *Publisher {
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	p := &Publisher{
		redisClient: rdb,
		inputChan:   inputChan,
		stopChan:    make(chan struct{}),
	}

	async.Go(func() { p.run() })
	return p
}

func (p *Publisher) run() {
	log.Info().Msg("publisher started")
	defer log.Info().Msg("publisher stopped")

	for {
		select {
		case candle := <-p.inputChan:
			p.publish(candle)
		case <-p.stopChan:
			p.redisClient.Close()
			return
		}
	}
}

func (p *Publisher) publish(candle *v1.Candle) {
	ctx := context.Background()
	channel := fmt.Sprintf("candles:%s", candle.InstrumentPair)

	payload, err := json.Marshal(candle)
	if err != nil {
		log.Error().Err(err).Msg("failed to marshal candle")
		return
	}

	if err := p.redisClient.Publish(ctx, channel, payload).Err(); err != nil {
		log.Error().Err(err).Str("channel", channel).Msg("failed to publish candle to redis")
	}
}

func (p *Publisher) Stop() {
	close(p.stopChan)
}
