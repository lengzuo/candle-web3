package external

import (
	"context"
	"encoding/json"
	binanceconnector "hermeneutic/binance-connector"
	"hermeneutic/internal/dto"
	"hermeneutic/utils/async"
	"strings"
	"sync"
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
	var wg sync.WaitGroup
	wg.Add(1)
	async.Go(func() {
		defer wg.Done()
		c.readMessages(ctx, conn)
	})

	// Start worker pool so that we can process the incoming trade
	// concurrency
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

type AggTrade struct {
	Symbol    string `json:"s"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
	Timestamp int64  `json:"T"`
}

func (c *Connector) handleMessage(_ context.Context, msg []byte) {
	log.Debug().Msgf("process [binance]: %s", msg)
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

	timestamp := time.Unix(0, tradeData.Timestamp*int64(time.Millisecond))

	instrumentPair := binanceconnector.ToSymbol(tradeData.Symbol)
	if instrumentPair == "" {
		return
	}
	trade := dto.Trade{
		InstrumentPair: instrumentPair,
		Price:          price,
		Quantity:       quantity,
		Timestamp:      timestamp,
	}
	c.tradeChan <- trade
}
