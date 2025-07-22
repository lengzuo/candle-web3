package aggregator

import (
	v1 "hermeneutic/pkg/proto/v1"
	"hermeneutic/utils/async"
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
	outputChan    chan *v1.Candle
	stopChan      chan struct{}
	activeCandles map[string]*candleBuilder
	mu            sync.RWMutex
}

func NewAggregator(interval time.Duration) *Aggregator {
	agg := &Aggregator{
		interval:      interval,
		tradeChan:     make(chan Trade, 1000),
		outputChan:    make(chan *v1.Candle, 100),
		stopChan:      make(chan struct{}),
		activeCandles: make(map[string]*candleBuilder),
	}
	async.Go(func() { agg.run() })
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

		// Send the finalized candle to the internal output channel.
		a.outputChan <- candle

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

// OutputChannel returns the channel where finalized candles are sent.
// This will be consumed by the Broadcaster.
func (a *Aggregator) OutputChannel() <-chan *v1.Candle {
	return a.outputChan
}

func (a *Aggregator) Stop() {
	close(a.stopChan)
	// Close the output channel after the run loop has exited
	// to signal to the broadcaster that no more candles will come.
	// This needs to be done carefully to avoid closing a channel
	// that might still be read from by the broadcaster.
	// For now, we'll rely on the main context cancellation to stop
	// the broadcaster, which will then stop reading from this channel.
	// A more robust solution might involve a separate done channel for the aggregator's run loop.
}
