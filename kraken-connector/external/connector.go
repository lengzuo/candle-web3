package external

import (
	"context"
	"encoding/json"
	"hermeneutic/internal/dto"
	krakenconnector "hermeneutic/kraken-connector"
	"hermeneutic/utils/async"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
)

const (
	webSocketURL = "wss://ws.kraken.com/v2"
)

type Connector struct {
	producer   *kafka.Writer
	pairs      []string
	tradeChan  chan dto.Trade
	numWorkers int
}

func NewConnector(producer *kafka.Writer, pairs []string, numWorkers int) *Connector {
	return &Connector{
		producer:   producer,
		pairs:      pairs,
		tradeChan:  make(chan dto.Trade, 1000),
		numWorkers: numWorkers,
	}
}

func (c *Connector) Start(ctx context.Context) {
	log.Info().Msg("starting kraken connector")
	defer log.Info().Msg("kraken connector stopped")

	conn, _, err := websocket.DefaultDialer.Dial(webSocketURL, nil)
	if err != nil {
		log.Error().Err(err).Msg("failed to connect to webSocket")
		return
	}
	defer conn.Close()

	subscribeMsg := map[string]interface{}{
		"method": "subscribe",
		"params": map[string]interface{}{
			"channel": "trade",
			"symbol":  c.pairs,
		},
	}
	if err := conn.WriteJSON(subscribeMsg); err != nil {
		log.Error().Err(err).Msg("failed to subscribe to streams")
		return
	}

	log.Info().Strs("streams", c.pairs).Msg("subscribed to trade streams")
	var wg sync.WaitGroup

	wg.Add(1)
	async.Go(func() {
		defer wg.Done()
		c.readMessages(ctx, conn)
	})

	wg.Add(c.numWorkers)
	for range c.numWorkers {
		async.Go(func() {
			defer wg.Done()
			c.worker(ctx)
		})
	}

	<-ctx.Done()
	wg.Wait()
	close(c.tradeChan)
}

func (c *Connector) readMessages(ctx context.Context, conn *websocket.Conn) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Error().Err(err).Msg("error reading from webSocket")
				return
			}
			c.handleMessage(ctx, message)
		}
	}
}

func (c *Connector) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case trade := <-c.tradeChan:
			payload, err := json.Marshal(trade)
			if err != nil {
				log.Error().Err(err).Msg("failed to marshal trade")
				continue
			}

			err = c.producer.WriteMessages(ctx, kafka.Message{
				Key:   []byte(trade.InstrumentPair),
				Value: payload,
			})
			if err != nil {
				log.Error().Err(err).Msg("failed to write message to kafka")
			}
		}
	}
}

type KrakenV2Trade struct {
	Symbol    string `json:"symbol"`
	Price     string `json:"price"`
	Qty       string `json:"qty"`
	Timestamp string `json:"timestamp"`
}

type KrakenV2Message struct {
	Channel string          `json:"channel"`
	Data    []KrakenV2Trade `json:"data"`
}

func (c *Connector) handleMessage(_ context.Context, msg []byte) {
	log.Debug().Msgf("process [kraken]: %s", msg)
	var v2Msg KrakenV2Message
	if err := json.Unmarshal(msg, &v2Msg); err != nil {
		// Ignore non-v2 messages or errors (e.g., heartbeats, status messages)
		return
	}

	if v2Msg.Channel != "trade" {
		// Ignore non-trade messages (e.g., "heartbeat", "status")
		return
	}

	for _, tradeData := range v2Msg.Data {
		price, err := decimal.NewFromString(tradeData.Price)
		if err != nil {
			log.Warn().Err(err).Str("price", tradeData.Price).Msg("could not parse trade price")
			continue
		}
		quantity, err := decimal.NewFromString(tradeData.Qty)
		if err != nil {
			log.Warn().Err(err).Str("quantity", tradeData.Qty).Msg("could not parse trade quantity")
			continue
		}

		// Kraken v2 timestamp is ISO 8601 string, not Unix timestamp
		timestamp, err := time.Parse(time.RFC3339Nano, tradeData.Timestamp)
		if err != nil {
			log.Warn().Err(err).Str("timestamp", tradeData.Timestamp).Msg("could not parse trade timestamp")
			continue
		}

		instrumentPair := krakenconnector.ToSymbol(tradeData.Symbol)
		if instrumentPair == "" {
			continue
		}
		trade := dto.Trade{
			InstrumentPair: instrumentPair,
			Price:          price,
			Quantity:       quantity,
			Timestamp:      timestamp,
		}
		log.Debug().Msgf("trade: %#v", trade)

		c.tradeChan <- trade
	}
}
