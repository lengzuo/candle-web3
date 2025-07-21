package external

import (
	"context"
	"encoding/json"
	binanceconnector "hermeneutic/binance-connector"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
)

const (
	webSocketURL = "wss://stream.binance.com:9443/ws"
)

type Connector struct {
	producer *kafka.Writer
	pairs    []string
}

func NewConnector(producer *kafka.Writer, pairs []string) *Connector {
	return &Connector{
		producer: producer,
		pairs:    pairs,
	}
}

func (c *Connector) Start(ctx context.Context) {
	log.Info().Msg("starting binance connector")
	defer log.Info().Msg("binance connector stopped")

	var streams []string
	for _, p := range c.pairs {
		streams = append(streams, strings.ToLower(strings.Replace(p, "-", "", 1))+"@aggTrade")
	}

	conn, _, err := websocket.DefaultDialer.Dial(webSocketURL, nil)
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

	go func() {
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Error().Err(err).Msg("error reading from webSocket")
				return
			}
			c.handleMessage(ctx, message)
		}
	}()

	<-ctx.Done()
}

type Trade struct {
	InstrumentPair string          `json:"instrument_pair"`
	Price          decimal.Decimal `json:"price"`
	Quantity       decimal.Decimal `json:"quantity"`
	Timestamp      time.Time       `json:"timestamp"`
}

type AggTrade struct {
	Symbol    string `json:"s"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
	Timestamp int64  `json:"T"`
}

func (c *Connector) handleMessage(ctx context.Context, msg []byte) {
	var tradeData AggTrade
	if err := json.Unmarshal(msg, &tradeData); err != nil {
		log.Warn().Err(err).Msg("failed to unmarshal trade data")
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

	timestamp := time.Unix(0, tradeData.Timestamp*int64(time.Millisecond))

	pair := tradeData.Symbol[:len(tradeData.Symbol)-4] + "-USDT"

	trade := Trade{
		InstrumentPair: pair,
		Price:          price,
		Quantity:       quantity,
		Timestamp:      timestamp,
	}

	payload, err := json.Marshal(trade)
	if err != nil {
		log.Error().Err(err).Msg("failed to marshal trade")
		return
	}

	err = c.producer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(binanceconnector.ToInternal(trade.InstrumentPair)),
		Value: payload,
	})
	if err != nil {
		log.Error().Err(err).Msg("failed to write message to kafka")
	}
}
