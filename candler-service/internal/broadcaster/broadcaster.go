package broadcaster

import (
	"context"
	"encoding/json"
	"fmt"
	v1 "hermeneutic/pkg/proto/v1"
	"hermeneutic/utils/async"
	"sync"

	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
)

type Broadcaster struct {
	redisClient      *redis.Client
	subscriptions    map[string]map[chan *v1.Candle]struct{}
	mu               sync.RWMutex
	redisCancelFuncs map[string]context.CancelFunc
	registerChan     chan *subscription
	unregisterChan   chan *subscription
}

type subscription struct {
	pair string
	ch   chan *v1.Candle
}

func NewBroadcaster(redisAddr string) *Broadcaster {
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	b := &Broadcaster{
		redisClient:      rdb,
		subscriptions:    make(map[string]map[chan *v1.Candle]struct{}),
		redisCancelFuncs: make(map[string]context.CancelFunc),
		registerChan:     make(chan *subscription),
		unregisterChan:   make(chan *subscription),
	}

	go b.run()
	return b
}

func (b *Broadcaster) run() {
	for {
		select {
		case s := <-b.registerChan:
			b.addSubscription(s.pair, s.ch)
		case s := <-b.unregisterChan:
			b.removeSubscription(s.pair, s.ch)
		}
	}
}

func (b *Broadcaster) addSubscription(pair string, ch chan *v1.Candle) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.subscriptions[pair]; !ok {
		b.subscriptions[pair] = make(map[chan *v1.Candle]struct{})
		// Start a cancel context to cancel when subscriber is stop
		ctx, cancel := context.WithCancel(context.Background())
		b.redisCancelFuncs[pair] = cancel
		async.Go(func() { b.subscribeToRedis(ctx, pair) })
	}
	b.subscriptions[pair][ch] = struct{}{}
}

func (b *Broadcaster) removeSubscription(pair string, ch chan *v1.Candle) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if chans, ok := b.subscriptions[pair]; ok {
		delete(chans, ch)
		if len(chans) == 0 {
			delete(b.subscriptions, pair)
			if cancel, ok := b.redisCancelFuncs[pair]; ok {
				cancel()
				delete(b.redisCancelFuncs, pair)
			}
		}
	}
}

func (b *Broadcaster) subscribeToRedis(ctx context.Context, pair string) {
	channel := fmt.Sprintf("candles:%s", pair)
	pubsub := b.redisClient.Subscribe(ctx, channel)
	defer pubsub.Close()

	ch := pubsub.Channel()

	for {
		select {
		case <-ctx.Done():
			log.Info().Str("pair", pair).Msg("redis subscription context cancelled, stopping goroutine.")
			return
		case msg := <-ch:
			var candle v1.Candle
			if err := json.Unmarshal([]byte(msg.Payload), &candle); err != nil {
				log.Warn().Err(err).Msg("failed to unmarshal candle from redis")
				continue
			}

			b.mu.RLock()
			if chans, ok := b.subscriptions[pair]; ok {
				for clientChan := range chans {
					select {
					case clientChan <- &candle:
						// Successfully sent
					default:
						log.Warn().Str("pair", pair).Msg("client channel full, dropping candle.")
					}
				}
			}
			b.mu.RUnlock()
		}
	}
}

func (b *Broadcaster) Subscribe(pair string, ch chan *v1.Candle) {
	b.registerChan <- &subscription{pair: pair, ch: ch}
}

func (b *Broadcaster) Unsubscribe(pair string, ch chan *v1.Candle) {
	b.unregisterChan <- &subscription{pair: pair, ch: ch}
}
