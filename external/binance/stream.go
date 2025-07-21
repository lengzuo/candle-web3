package binance

import (
	"context"
	"encoding/json"
	"hermeneutic/internal/candles/aggregator"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
)

const (
	WebSocketURL = "wss://stream.binance.com:9443/ws"
)

type Connector struct {
	aggregator *aggregator.Aggregator
	pairs      []string
	stopChan   chan struct{}
}

func NewConnector(agg *aggregator.Aggregator, pairs []string) *Connector {
	return &Connector{
		aggregator: agg,
		pairs:      pairs,
		stopChan:   make(chan struct{}),
	}
}

func (c *Connector) Start(ctx context.Context) {
	go c.run(ctx)
}

func (c *Connector) Stop() {
	close(c.stopChan)
}

func (c *Connector) run(ctx context.Context) {
	log.Info().Msg("starting connector")
	defer log.Info().Msg("connector stopped")

	var streams []string
	for _, p := range c.pairs {
		streams = append(streams, strings.ToLower(strings.Replace(p, "-", "", 1))+"@aggTrade")
	}

	conn, _, err := websocket.DefaultDialer.Dial(WebSocketURL, nil)
	if err != nil {
		log.Error().Err(err).Msg("failed to connect to webSocket")
		return
	}
	defer conn.Close()

	subscribeMsg := map[string]interface{}{
		"method": "SUBSCRIBE",
		"params": streams,
		"id":     1,
	}
	if err := conn.WriteJSON(subscribeMsg); err != nil {
		log.Error().Err(err).Msg("failed to subscribe to streams")
		return
	}

	log.Info().Strs("streams", streams).Msg("subscribed to trade streams")

	for {
		select {
		case <-c.stopChan:
			return
		case <-ctx.Done():
			return
		default:
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Error().Err(err).Msg("error reading from webSocket")
				return
			}
			c.handleMessage(message)
		}
	}
}

type AggTrade struct {
	Symbol    string `json:"s"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
	Timestamp int64  `json:"T"`
}

func (c *Connector) handleMessage(msg []byte) {
	var tradeData AggTrade
	if err := json.Unmarshal(msg, &tradeData); err != nil {
		return
	}

	price, err := decimal.NewFromString(tradeData.Price)
	if err != nil {
		log.Warn().Err(err).Str("price", tradeData.Price).Msg("could not parse trade price")
		return
	}
	quantity, err := decimal.NewFromString(tradeData.Quantity)
	if err != nil {
		log.Warn().Err(err).Str("quantity", tradeData.Quantity).Msg("could not parse trade quantity")
		return
	}

	// Binance provides timestamp in milliseconds
	timestamp := time.Unix(0, tradeData.Timestamp*int64(time.Millisecond))

	pair := tradeData.Symbol[:len(tradeData.Symbol)-4] + "-USDT"

	trade := aggregator.Trade{
		InstrumentPair: pair,
		Price:          price,
		Quantity:       quantity,
		Timestamp:      timestamp,
	}

	c.aggregator.AddTrade(trade)
}
