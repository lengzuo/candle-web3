package external

import (
	"context"
	"encoding/json"
	coinbaseconnector "hermeneutic/coinbase-connector"
	"hermeneutic/internal/dto"
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

type CoinbaseMatch struct {
	Type      string `json:"type"`
	TradeID   int64  `json:"trade_id"`
	ProductID string `json:"product_id"`
	Price     string `json:"price"`
	Size      string `json:"size"`
	Time      string `json:"time"`
}

func (c *Connector) handleMessage(_ context.Context, msg []byte) {
	log.Debug().Msgf("process [coinbase]: %s", msg)
	var match CoinbaseMatch
	if err := json.Unmarshal(msg, &match); err != nil {
		return
	}

	if match.Type != "match" && match.Type != "last_match" {
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

	instrumentPair := coinbaseconnector.ToSymbol(match.ProductID)
	if instrumentPair == "" {
		return
	}
	trade := dto.Trade{
		InstrumentPair: instrumentPair.String(),
		Price:          price,
		Quantity:       quantity,
		Timestamp:      timestamp,
		TradeID:        match.TradeID,
	}

	c.tradeChan <- trade
}
