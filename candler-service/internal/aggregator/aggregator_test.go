package aggregator

import (
	"hermeneutic/internal/dto"
	v1 "hermeneutic/pkg/proto/v1"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestFinalizeCandles_ArriveAtDiffOrder(t *testing.T) {
	interval := time.Minute
	agg := NewAggregator(interval)
	defer agg.Stop()

	tradeTime1, _ := time.Parse(time.RFC3339Nano, "2025-07-23T04:55:03.828369Z")
	tradeTime2, _ := time.Parse(time.RFC3339Nano, "2025-07-23T04:55:03.830873Z")

	trades := []dto.Trade{
		{
			InstrumentPair: "ETH-USDT",
			TradeID:        29919188,
			Price:          decimal.NewFromFloat(3730.47),
			Quantity:       decimal.NewFromFloat(0.10705172),
			Timestamp:      tradeTime1,
		},
		{
			InstrumentPair: "ETH-USDT",
			TradeID:        29919187,
			Price:          decimal.NewFromFloat(3731.03),
			Quantity:       decimal.NewFromFloat(0.02692),
			Timestamp:      tradeTime1,
		},
		{
			InstrumentPair: "ETH-USDT",
			TradeID:        29919189,
			Price:          decimal.NewFromFloat(3730.47),
			Quantity:       decimal.NewFromFloat(0.02692000),
			Timestamp:      tradeTime2,
		},
	}

	for _, trade := range trades {
		agg.processTrade(trade)
	}

	cutoffTime := tradeTime1.Add(interval)
	agg.finalizeCandles(cutoffTime)

	select {
	case finalizedCandle := <-agg.OutputChannel():
		assert.Equal(t, "3731.03", finalizedCandle.Open, "Open price should be from the first trade")
		assert.Equal(t, "3731.03", finalizedCandle.High, "High price is incorrect")
		assert.Equal(t, "3730.47", finalizedCandle.Low, "Low price is incorrect")
		assert.Equal(t, "3730.47", finalizedCandle.Close, "Close price should be from the last trade")
		expectedVolume := decimal.NewFromFloat(0.16089172)
		assert.Equal(t, expectedVolume.String(), finalizedCandle.Volume, "Volume is incorrect")
		expectedCandleTime := tradeTime1.Truncate(interval)
		assert.Equal(t, expectedCandleTime.Unix(), finalizedCandle.Timestamp.Seconds, "Candle timestamp is incorrect")

	case <-time.After(1 * time.Second):
		t.Fatal("Test timed out waiting for a candle to be finalized")
	}
}

func TestFinalizeCandles_DeterministicOrder(t *testing.T) {
	interval := time.Minute
	agg := NewAggregator(interval)
	defer agg.Stop()

	ts, _ := time.Parse(time.RFC3339Nano, "2025-07-23T05:00:00Z")
	trades := []dto.Trade{
		{InstrumentPair: "BTC-USDT", TradeID: 1, Price: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(1), Timestamp: ts},
		{InstrumentPair: "ETH-USDT", TradeID: 1, Price: decimal.NewFromInt(50), Quantity: decimal.NewFromInt(1), Timestamp: ts},
	}

	agg.processTrade(trades[1])
	agg.processTrade(trades[0])

	cutoffTime := ts.Add(interval)
	agg.finalizeCandles(cutoffTime)

	var receivedCandles []*v1.Candle
	for i := 0; i < 2; i++ {
		select {
		case candle := <-agg.OutputChannel():
			receivedCandles = append(receivedCandles, candle)
		case <-time.After(1 * time.Second):
			t.Fatalf("Timed out waiting for candle #%d", i+1)
		}
	}

	assert.Len(t, receivedCandles, 2)
	assert.Equal(t, "BTC-USDT", receivedCandles[0].InstrumentPair, "First candle should be BTC-USDT")
	assert.Equal(t, "ETH-USDT", receivedCandles[1].InstrumentPair, "Second candle should be ETH-USDT")
}

func TestFinalizeCandles_ConsolidatedFromMultipleExchanges(t *testing.T) {
	interval := time.Minute
	agg := NewAggregator(interval)
	defer agg.Stop()

	baseTime, _ := time.Parse(time.RFC3339Nano, "2025-07-23T08:00:00Z")

	trades := []dto.Trade{
		{
			InstrumentPair: "BTC-USDT",
			// Exchange:       "Binance",
			Price:     decimal.NewFromFloat(60000.0),
			Quantity:  decimal.NewFromFloat(0.1),
			Timestamp: baseTime,
		},
		{
			InstrumentPair: "BTC-USDT",
			// Exchange:       "Kraken",
			Price:     decimal.NewFromFloat(62000.0),
			Quantity:  decimal.NewFromFloat(0.2),
			Timestamp: baseTime.Add(1 * time.Second),
		},
		{
			InstrumentPair: "BTC-USDT",
			// Exchange:       "Coinbase",
			Price:     decimal.NewFromFloat(59000.0),
			Quantity:  decimal.NewFromFloat(0.15),
			Timestamp: baseTime.Add(2 * time.Second),
		},
		{
			InstrumentPair: "BTC-USDT",
			// Exchange:       "Binance",
			Price:     decimal.NewFromFloat(61000.0),
			Quantity:  decimal.NewFromFloat(0.05),
			Timestamp: baseTime.Add(3 * time.Second),
		},
	}

	for _, trade := range trades {
		agg.processTrade(trade)
	}

	cutoffTime := baseTime.Add(interval)
	agg.finalizeCandles(cutoffTime)

	select {
	case finalizedCandle := <-agg.OutputChannel():
		assert.Equal(t, "BTC-USDT", finalizedCandle.InstrumentPair)
		assert.Equal(t, "60000", finalizedCandle.Open, "Open price is incorrect")
		assert.Equal(t, "62000", finalizedCandle.High, "High price is incorrect")
		assert.Equal(t, "59000", finalizedCandle.Low, "Low price is incorrect")
		assert.Equal(t, "61000", finalizedCandle.Close, "Close price is incorrect")

		expectedVolume := decimal.NewFromFloat(0.1 + 0.2 + 0.15 + 0.05)
		assert.Equal(t, expectedVolume.String(), finalizedCandle.Volume, "Volume is incorrect")

		expectedCandleTime := baseTime.Truncate(interval)
		assert.Equal(t, expectedCandleTime.Unix(), finalizedCandle.Timestamp.Seconds, "Candle timestamp is incorrect")

	case <-time.After(2 * time.Second):
		t.Fatal("Test timed out waiting for a candle to be finalized")
	}
}
