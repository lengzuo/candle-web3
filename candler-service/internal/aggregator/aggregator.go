package aggregator

import (
	"hermeneutic/internal/dto"
	v1 "hermeneutic/pkg/proto/v1"
	"hermeneutic/utils/async"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// pairCandleData holds all the state for a single instrument pair,
// allowing for granular locking.

// TODO: Persist this state to a durable store (e.g., Redis, RocksDB) to ensure
// fault tolerance. If the service restarts, it should be able to recover the
// active candles and watermarks to avoid data loss and generate accurate candles.
type pairCandleData struct {
	mu                sync.Mutex
	activeCandles     map[int64]*dto.ActiveCandle
	watermark         time.Time
	lastFinalizedTime time.Time
}

type Aggregator struct {
	interval time.Duration
	// grace period for late trades
	flushDelay time.Duration
	tradeChan  chan dto.Trade
	outputChan chan *v1.Candle
	stopChan   chan struct{}

	// candleData holds the state for each instrument pair.
	// The map itself is protected by mapMutex.
	candleData map[string]*pairCandleData
	mapMutex   sync.RWMutex
}

func NewAggregator(interval time.Duration, flushDelay time.Duration) *Aggregator {
	agg := &Aggregator{
		interval:   interval,
		flushDelay: flushDelay,
		tradeChan:  make(chan dto.Trade, 1000),
		outputChan: make(chan *v1.Candle, 100),
		stopChan:   make(chan struct{}),
		candleData: make(map[string]*pairCandleData),
	}
	async.Go(func() { agg.run() })
	return agg
}

func (a *Aggregator) run() {
	log.Info().Msg("aggregator started")
	defer log.Info().Msg("aggregator stopped")

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

// getOrCreatePairData safely retrieves or creates a new pairCandleData for the given instrument pair.
// This uses a read-lock for the common case (data exists) and upgrades to a write-lock only when creating.
func (a *Aggregator) getOrCreatePairData(pair string) *pairCandleData {
	a.mapMutex.RLock()
	data, ok := a.candleData[pair]
	a.mapMutex.RUnlock()
	if ok {
		return data
	}

	a.mapMutex.Lock()
	defer a.mapMutex.Unlock()
	// Double-check in case another goroutine created it while we were waiting for the lock.
	if data, ok = a.candleData[pair]; ok {
		return data
	}

	data = &pairCandleData{
		activeCandles: make(map[int64]*dto.ActiveCandle),
	}
	a.candleData[pair] = data
	return data
}

func (a *Aggregator) processTrade(trade dto.Trade) {
	pairData := a.getOrCreatePairData(trade.InstrumentPair)

	pairData.mu.Lock()
	defer pairData.mu.Unlock()

	// Update watermark for the instrument pair if the new trade is later
	if trade.Timestamp.After(pairData.watermark) {
		pairData.watermark = trade.Timestamp
	}

	candleTime := trade.Timestamp.Truncate(a.interval)
	ts := candleTime.UnixNano()

	// To prevent processing trades for already finalized candles,
	// check against the pair-specific last finalized time.
	if !pairData.lastFinalizedTime.IsZero() && candleTime.Before(pairData.lastFinalizedTime) {
		// TODO: Implement a DLQ to send these trades for later processing
		// instead of just discarding them. This ensures data is not lost.
		log.Warn().
			Str("instrument", trade.InstrumentPair).
			Int64("trade_id", trade.TradeID).
			Time("trade_ts", trade.Timestamp).
			Time("candle_ts", candleTime).
			Time("last_finalized_ts", pairData.lastFinalizedTime).
			Msg("received out-of-order trade for an already finalized candle. Discarding.")
		return
	}

	activeCandle, ok := pairData.activeCandles[ts]
	if !ok {
		activeCandle = dto.NewActiveCandle()
		pairData.activeCandles[ts] = activeCandle
	}

	activeCandle.AddTrade(&trade)
}

func (a *Aggregator) finalizeCandles() {
	// Get a snapshot of the pairs to iterate over.
	// We don't want to hold the map lock while processing each pair.
	a.mapMutex.RLock()
	pairs := make([]string, 0, len(a.candleData))
	for p := range a.candleData {
		pairs = append(pairs, p)
	}
	a.mapMutex.RUnlock()
	slices.Sort(pairs)

	for _, pair := range pairs {
		pairData := a.getOrCreatePairData(pair)

		pairData.mu.Lock()
		// Watermark is meaningless if it's zero.
		if pairData.watermark.IsZero() {
			pairData.mu.Unlock()
			continue
		}

		// Any candle before this time is safe to finalize.
		safeFlushTime := pairData.watermark.Add(-a.flushDelay)

		// Collect timestamps to finalize without holding the lock during sorting.
		timestampsToFinalize := make([]int64, 0)
		for ts := range pairData.activeCandles {
			candleStartTime := time.Unix(0, ts)
			if candleStartTime.Before(safeFlushTime) {
				timestampsToFinalize = append(timestampsToFinalize, ts)
			}
		}
		slices.Sort(timestampsToFinalize)

		for _, ts := range timestampsToFinalize {
			activeCandle := pairData.activeCandles[ts]
			if activeCandle.TradeCount > 0 {
				finalCandle := &v1.Candle{
					InstrumentPair: pair,
					Open:           activeCandle.Open().String(),
					High:           activeCandle.High.String(),
					Low:            activeCandle.Low.String(),
					Close:          activeCandle.Close().String(),
					Volume:         activeCandle.Volume.String(),
					Timestamp:      timestamppb.New(time.Unix(0, ts)),
				}
				log.Debug().Int64("first_trade", activeCandle.FirstTradeID()).
					Int64("last_trade", activeCandle.LastTradeID()).
					Msgf("candle_ts: %s", finalCandle.Timestamp)
				a.outputChan <- finalCandle
				pairData.lastFinalizedTime = time.Unix(0, ts)
			}
			delete(pairData.activeCandles, ts)
		}
		pairData.mu.Unlock()
	}
}

func (a *Aggregator) AddTrade(trade dto.Trade) {
	a.tradeChan <- trade
}

// OutputChannel returns the channel where finalized candles are sent.
// This will be consumed by the Broadcaster.
func (a *Aggregator) OutputChannel() <-chan *v1.Candle {
	return a.outputChan
}

func (a *Aggregator) Stop() {
	close(a.stopChan)
}
