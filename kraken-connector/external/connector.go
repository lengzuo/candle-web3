package external

import (
	"context"
	"encoding/json"
	"hermeneutic/internal/dto"
	krakenconnector "hermeneutic/kraken-connector"
	"hermeneutic/utils/async"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
)

const (
	webSocketURL    = "wss://ws.kraken.com"
	channel_id      = 0
	trade_info      = 1
	channel_name    = 2
	trade_pair      = 3
	trade_price     = 0
	trade_volumne   = 1
	trade_timestamp = 2
	trade_side      = 3
	trade_orderType = 4
	trade_misc      = 5
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
		"event": "subscribe",
		"pair":  c.pairs,
		"subscription": map[string]string{
			"name": "trade",
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

func (c *Connector) handleMessage(_ context.Context, msg []byte) {
	log.Debug().Msgf("process [kraken]: %s", msg)
	var tradeData []any
	if err := json.Unmarshal(msg, &tradeData); err != nil {
		return
	}

	// Ensure the message is a trade message, which has a specific length and structure.
	if len(tradeData) != 4 || tradeData[channel_name] != "trade" {
		return
	}

	trades, ok := tradeData[trade_info].([]any)
	if !ok {
		return
	}

	pair, ok := tradeData[trade_pair].(string)
	if !ok {
		return
	}

	for _, t := range trades {
		tradeInfo, ok := t.([]any)
		if !ok || len(tradeInfo) < 3 {
			continue
		}

		priceStr, pOk := tradeInfo[trade_price].(string)
		quantityStr, qOk := tradeInfo[trade_volumne].(string)
		timestampStr, tOk := tradeInfo[trade_timestamp].(string)

		if !pOk || !qOk || !tOk {
			continue
		}

		price, err := decimal.NewFromString(priceStr)
		if err != nil {
			log.Warn().Err(err).Str("price", priceStr).Msg("could not parse trade price")
			continue
		}
		quantity, err := decimal.NewFromString(quantityStr)
		if err != nil {
			log.Warn().Err(err).Str("quantity", quantityStr).Msg("could not parse trade quantity")
			continue
		}

		timestamp, err := ParseTimestamp(timestampStr)
		if err != nil {
			log.Warn().Err(err).Str("timestamp", timestampStr).Msg("could not parse trade timestamp")
			continue
		}

		trade := dto.Trade{
			InstrumentPair: krakenconnector.ToSymbol(pair),
			Price:          price,
			Quantity:       quantity,
			Timestamp:      timestamp,
		}

		c.tradeChan <- trade
	}
}
