package dto

import (
	"github.com/shopspring/decimal"
)

type ActiveCandle struct {
	firstTrade *Trade
	lastTrade  *Trade
	High       decimal.Decimal
	Low        decimal.Decimal
	Volume     decimal.Decimal
	TradeCount int
}

func NewActiveCandle() *ActiveCandle {
	return &ActiveCandle{}
}

// isTradeEarlier checks if trade `a` happened before trade `b`.
func isTradeEarlier(a, b *Trade) bool {
	if a.Timestamp.Equal(b.Timestamp) {
		return a.TradeID < b.TradeID
	}
	return a.Timestamp.Before(b.Timestamp)
}

// isTradeLater checks if trade `a` happened after trade `b`.
func isTradeLater(a, b *Trade) bool {
	if a.Timestamp.Equal(b.Timestamp) {
		return a.TradeID > b.TradeID
	}
	return a.Timestamp.After(b.Timestamp)
}

func (ac *ActiveCandle) AddTrade(trade *Trade) {
	if ac.TradeCount == 0 {
		ac.firstTrade = trade
		ac.lastTrade = trade
		ac.High = trade.Price
		ac.Low = trade.Price
	} else {
		// Update first trade if the new one is earlier.
		if isTradeEarlier(trade, ac.firstTrade) {
			ac.firstTrade = trade
		}
		// Update last trade if the new one is later.
		if isTradeLater(trade, ac.lastTrade) {
			ac.lastTrade = trade
		}

		// Update high and low.
		if trade.Price.GreaterThan(ac.High) {
			ac.High = trade.Price
		}
		if trade.Price.LessThan(ac.Low) {
			ac.Low = trade.Price
		}
	}

	ac.Volume = ac.Volume.Add(trade.Quantity)
	ac.TradeCount++
}

// Open returns the opening price, which is the price of the first trade.
func (ac *ActiveCandle) Open() decimal.Decimal {
	if ac.firstTrade == nil {
		return decimal.Zero
	}
	return ac.firstTrade.Price
}

// Close returns the closing price, which is the price of the last trade.
func (ac *ActiveCandle) Close() decimal.Decimal {
	if ac.lastTrade == nil {
		return decimal.Zero
	}
	return ac.lastTrade.Price
}

// Open returns the opening price, which is the price of the first trade.
func (ac *ActiveCandle) FirstTradeID() int64 {
	if ac.firstTrade == nil {
		return 0
	}
	return ac.firstTrade.TradeID
}

// Close returns the closing price, which is the price of the last trade.
func (ac *ActiveCandle) LastTradeID() int64 {
	if ac.lastTrade == nil {
		return 0
	}
	return ac.lastTrade.TradeID
}
