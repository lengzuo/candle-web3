package broadcaster

import (
	"sync"

	v1 "hermeneutic/pkg/proto/v1"
	"hermeneutic/utils/async"

	"github.com/rs/zerolog/log"
)

// Broadcaster manages the fan-out of candles to multiple gRPC clients.
type Broadcaster struct {
	clients    sync.Map // Stores client channels: map[client_id]chan *v1.Candle
	inputChan  <-chan *v1.Candle
	register   chan chan *v1.Candle
	unregister chan chan *v1.Candle
}

// NewBroadcaster creates and returns a new Broadcaster instance.
func NewBroadcaster(inputChan <-chan *v1.Candle) *Broadcaster {
	b := &Broadcaster{
		clients:    sync.Map{},
		inputChan:  inputChan,
		register:   make(chan chan *v1.Candle),
		unregister: make(chan chan *v1.Candle),
	}
	async.Go(func() { b.run() }) // Start the main run loop in a goroutine
	return b
}

// run is the main loop of the Broadcaster. It listens for new candles
// and client registration/unregistration requests.
func (b *Broadcaster) run() {
	log.Info().Msg("broadcaster started")
	defer log.Info().Msg("broadcaster stopped")

	for {
		select {
		case clientChan := <-b.register:
			// A new client wants to subscribe
			b.clients.Store(clientChan, true)
			log.Debug().Msg("new client registered with broadcaster")
		case clientChan := <-b.unregister:
			// A client wants to unsubscribe
			b.clients.Delete(clientChan)
			close(clientChan)
			log.Debug().Msg("client unregistered from broadcaster")
		case candle, ok := <-b.inputChan:
			// A new candle is available from the aggregator
			if !ok {
				// Input channel closed, gracefully shut down
				log.Info().Msg("broadcaster input channel closed, shutting down clients")
				b.clients.Range(func(key, value any) bool {
					clientChan, ok := key.(chan *v1.Candle)
					if ok {
						close(clientChan)
					}
					b.clients.Delete(key)
					return true
				})
				return
			}
			// Fan out the candle to all active clients
			b.clients.Range(func(key, value any) bool {
				clientChan, ok := key.(chan *v1.Candle)
				if !ok {
					return true
				}
				select {
				case clientChan <- candle:
					// Successfully sent to client
				default:
					// Client's channel is full, drop message and log warning
					log.Warn().Msg("client channel full, dropping candle")
				}
				return true
			})
		}
	}
}

// RegisterClient is called by the gRPC server to register a new client's channel.
func (b *Broadcaster) RegisterClient(clientChan chan *v1.Candle) {
	b.register <- clientChan
}

// UnregisterClient is called by the gRPC server to unregister a client's channel.
func (b *Broadcaster) UnregisterClient(clientChan chan *v1.Candle) {
	b.unregister <- clientChan
}
