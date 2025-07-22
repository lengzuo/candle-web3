package external

import (
	"context"
	"encoding/json"
	"hermeneutic/utils/async"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
)

const (
	webSocketURL = "wss://ws-feed.exchange.coinbase.com"
)

type Connector struct {
	producer  *kafka.Writer
	pairs     []string
	tradeChan chan Trade
}

func NewConnector(producer *kafka.Writer, pairs []string) *Connector {
	return &Connector{
		producer:  producer,
		pairs:     pairs,
		tradeChan: make(chan Trade, 1000),
	}
}

func (c *Connector) Start(ctx context.Context) {
	log.Info().Msg("starting coinbase connector")
	defer log.Info().Msg("coinbase connector stopped")

	conn, _, err := websocket.DefaultDialer.Dial(webSocketURL, nil)
	if err != nil {
		log.Error().Err(err).Msg("failed to connect to webSocket")
		return
	}
	defer conn.Close()

	subscribeMsg := map[string]interface{}{
		"type":        "subscribe",
		"product_ids": c.pairs,
		"channels":    []string{"matches"},
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

	wg.Add(1)
	async.Go(func() {
		defer wg.Done()
		c.writeMessages(ctx)
	})

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

func (c *Connector) writeMessages(ctx context.Context) {
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

type Trade struct {
	InstrumentPair string          `json:"instrument_pair"`
	Price          decimal.Decimal `json:"price"`
	Quantity       decimal.Decimal `json:"quantity"`
	Timestamp      time.Time       `json:"timestamp"`
}

type CoinbaseMatch struct {
	Type      string `json:"type"`
	ProductID string `json:"product_id"`
	Price     string `json:"price"`
	Size      string `json:"size"`
	Time      string `json:"time"`
}

func (c *Connector) handleMessage(_ context.Context, msg []byte) {
	var match CoinbaseMatch
	if err := json.Unmarshal(msg, &match); err != nil {
		return
	}

	if match.Type != "match" {
		return
	}

	price, err := decimal.NewFromString(match.Price)
	if err != nil {
		log.Warn().Err(err).Str("price", match.Price).Msg("could not parse trade price")
		return
	}
	quantity, err := decimal.NewFromString(match.Size)
	if err != nil {
		log.Warn().Err(err).Str("quantity", match.Size).Msg("could not parse trade quantity")
		return
	}

	timestamp, err := time.Parse(time.RFC3339Nano, match.Time)
	if err != nil {
		log.Warn().Err(err).Str("timestamp", match.Time).Msg("could not parse trade timestamp")
		return
	}

	trade := Trade{
		InstrumentPair: match.ProductID,
		Price:          price,
		Quantity:       quantity,
		Timestamp:      timestamp,
	}

	c.tradeChan <- trade
}
