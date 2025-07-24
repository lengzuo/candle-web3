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

type Aggregator struct {
	interval          time.Duration
	tradeChan         chan dto.Trade
	outputChan        chan *v1.Candle
	stopChan          chan struct{}
	activeCandles     map[string]map[int64]*dto.ActiveCandle // Changed
	mu                sync.RWMutex
	lastFinalizedTime map[string]time.Time
}

func NewAggregator(interval time.Duration) *Aggregator {
	agg := &Aggregator{
		interval:          interval,
		tradeChan:         make(chan dto.Trade, 1000),
		outputChan:        make(chan *v1.Candle, 100),
		stopChan:          make(chan struct{}),
		activeCandles:     make(map[string]map[int64]*dto.ActiveCandle), // Changed
		lastFinalizedTime: make(map[string]time.Time),
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
			a.finalizeCandles(time.Now())
		case <-a.stopChan:
			return
		}
	}
}

func (a *Aggregator) processTrade(trade dto.Trade) {
	a.mu.Lock()
	defer a.mu.Unlock()

	candleTime := trade.Timestamp.Truncate(a.interval)

	if lastFinalized, ok := a.lastFinalizedTime[trade.InstrumentPair]; ok {
		if !candleTime.After(lastFinalized) {
			// For fast path, we can simply ignore this and store all trade data into a timeseries database.
			log.Debug().
				Str("instrument", trade.InstrumentPair).
				Int64("trade_id", trade.TradeID).
				Time("trade_ts", trade.Timestamp).
				Time("candle_ts", candleTime).
				Time("last_finalized_ts", lastFinalized).
				Msg("received out-of-order trade for an already finalized candle. Discarding.")
			return
		}
	}

	ts := candleTime.UnixNano()

	pairCandles, ok := a.activeCandles[trade.InstrumentPair]
	if !ok {
		pairCandles = make(map[int64]*dto.ActiveCandle)
		a.activeCandles[trade.InstrumentPair] = pairCandles
	}

	activeCandle, ok := pairCandles[ts]
	if !ok {
		activeCandle = dto.NewActiveCandle()
		pairCandles[ts] = activeCandle
	}

	activeCandle.AddTrade(&trade)
}

func (a *Aggregator) finalizeCandles(now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()

	cutoffTime := now.Truncate(a.interval)

	sortedPairs := make([]string, 0, len(a.activeCandles))
	for pair := range a.activeCandles {
		sortedPairs = append(sortedPairs, pair)
	}
	slices.Sort(sortedPairs)

	for _, pair := range sortedPairs {
		pairCandles := a.activeCandles[pair]

		sortedTimestamps := make([]int64, 0, len(pairCandles))
		for ts := range pairCandles {
			sortedTimestamps = append(sortedTimestamps, ts)
		}
		slices.Sort(sortedTimestamps)

		for _, ts := range sortedTimestamps {
			candleStartTime := time.Unix(0, ts)
			if candleStartTime.Before(cutoffTime) {
				activeCandle := pairCandles[ts]

				if activeCandle.TradeCount > 0 {
					finalCandle := &v1.Candle{
						InstrumentPair: pair,
						Open:           activeCandle.Open().String(),
						High:           activeCandle.High.String(),
						Low:            activeCandle.Low.String(),
						Close:          activeCandle.Close().String(),
						Volume:         activeCandle.Volume.String(),
						Timestamp:      timestamppb.New(candleStartTime),
					}
					a.outputChan <- finalCandle
					a.lastFinalizedTime[pair] = candleStartTime
				}
				delete(pairCandles, ts)
			}
		}
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
