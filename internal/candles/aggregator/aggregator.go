package aggregator

import (
	v1 "hermeneutic/pkg/proto/v1"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Trade struct {
	InstrumentPair string
	Price          decimal.Decimal
	Quantity       decimal.Decimal
	Timestamp      time.Time
}

type candleBuilder struct {
	InstrumentPair string
	Open           decimal.Decimal
	High           decimal.Decimal
	Low            decimal.Decimal
	Close          decimal.Decimal
	Volume         decimal.Decimal
	TradeCount     int64
}

type Aggregator struct {
	interval      time.Duration
	tradeChan     chan Trade
	candleChan    chan *v1.Candle
	stopChan      chan struct{}
	activeCandles map[string]*candleBuilder
	mu            sync.RWMutex
}

func NewAggregator(interval time.Duration) *Aggregator {
	agg := &Aggregator{
		interval:      interval,
		tradeChan:     make(chan Trade, 1000),
		candleChan:    make(chan *v1.Candle, 100),
		stopChan:      make(chan struct{}),
		activeCandles: make(map[string]*candleBuilder),
	}
	go agg.run()
	return agg
}

func (a *Aggregator) run() {
	log.Info().Msg("aggregator started")
	defer log.Info().Msg("aggregator stopped")

	// This ticker determines when to finalize and emit a candle
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	for {
		select {
		case trade := <-a.tradeChan:
			a.processTrade(trade)
		case <-ticker.C:
			a.finalizeCandles()
		case <-a.stopChan:
			return
		}
	}
}

func (a *Aggregator) processTrade(trade Trade) {
	a.mu.Lock()
	defer a.mu.Unlock()

	candle, ok := a.activeCandles[trade.InstrumentPair]
	if !ok {
		// First trade for this interval, create a new candle.
		a.activeCandles[trade.InstrumentPair] = &candleBuilder{
			InstrumentPair: trade.InstrumentPair,
			Open:           trade.Price,
			High:           trade.Price,
			Low:            trade.Price,
			Close:          trade.Price,
			Volume:         trade.Quantity,
			TradeCount:     1,
		}
		return
	}

	if trade.Price.GreaterThan(candle.High) {
		candle.High = trade.Price
	}
	if trade.Price.LessThan(candle.Low) {
		candle.Low = trade.Price
	}

	candle.Close = trade.Price
	candle.Volume = candle.Volume.Add(trade.Quantity)
	candle.TradeCount++
}

// finalizeCandles sends the completed candles and resets for the next interval
func (a *Aggregator) finalizeCandles() {
	a.mu.Lock()
	if len(a.activeCandles) == 0 {
		a.mu.Unlock()
		return
	}

	buildersToFinalize := a.activeCandles
	a.activeCandles = make(map[string]*candleBuilder)
	a.mu.Unlock()

	log.Debug().Msgf("finalizing %d candles for the interval", len(buildersToFinalize))

	for pair, builder := range buildersToFinalize {
		candle := &v1.Candle{
			InstrumentPair: builder.InstrumentPair,
			Open:           builder.Open.String(),
			High:           builder.High.String(),
			Low:            builder.Low.String(),
			Close:          builder.Close.String(),
			Volume:         builder.Volume.String(),
			Timestamp:      timestamppb.New(time.Now().Truncate(a.interval)),
		}

		// Send the finalized candle to the grpc broadcaster.
		a.candleChan <- candle

		log.Debug().
			Str("pair", pair).
			Str("close", candle.Close).
			Int64("trade_count", builder.TradeCount).
			Msg("Finalized candle")
	}
}

func (a *Aggregator) AddTrade(trade Trade) {
	a.tradeChan <- trade
}

func (a *Aggregator) CandleChannel() <-chan *v1.Candle {
	return a.candleChan
}

func (a *Aggregator) Stop() {
	close(a.stopChan)
}
