package dto

import (
	"container/heap"

	"github.com/shopspring/decimal"
)

// MinTradeHeap is a min-heap of trades, ordered by timestamp and trade_id.
type MinTradeHeap []*Trade

func (h MinTradeHeap) Len() int {
	return len(h)
}

func (h MinTradeHeap) Less(i, j int) bool {
	if h[i].Timestamp.Equal(h[j].Timestamp) {
		return h[i].TradeID < h[j].TradeID
	}
	return h[i].Timestamp.Before(h[j].Timestamp)
}

func (h MinTradeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *MinTradeHeap) Push(x any) {
	*h = append(*h, x.(*Trade))
}

func (h *MinTradeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// MaxTradeHeap is a min-heap of trades, ordered by timestamp and trade_id.
type MaxTradeHeap []*Trade

func (h MaxTradeHeap) Len() int {
	return len(h)
}

func (h MaxTradeHeap) Less(i, j int) bool {
	if h[i].Timestamp.Equal(h[j].Timestamp) {
		return h[i].TradeID > h[j].TradeID
	}
	return h[i].Timestamp.After(h[j].Timestamp)
}

func (h MaxTradeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *MaxTradeHeap) Push(x any) {
	*h = append(*h, x.(*Trade))
}

func (h *MaxTradeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type ActiveCandle struct {
	MinHeap    MinTradeHeap
	MaxHeap    MaxTradeHeap
	High       decimal.Decimal
	Low        decimal.Decimal
	Volume     decimal.Decimal
	TradeCount int
}

func NewActiveCandle() *ActiveCandle {
	ac := &ActiveCandle{
		MinHeap: make(MinTradeHeap, 0),
		MaxHeap: make(MaxTradeHeap, 0),
	}
	heap.Init(&ac.MinHeap)
	heap.Init(&ac.MaxHeap)
	return ac
}

func (ac *ActiveCandle) AddTrade(trade *Trade) {
	heap.Push(&ac.MinHeap, trade)
	heap.Push(&ac.MaxHeap, trade)

	if ac.TradeCount == 0 {
		ac.High = trade.Price
		ac.Low = trade.Price
	} else {
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

// Open returns the opening price from the min-heap.
func (ac *ActiveCandle) Open() decimal.Decimal {
	return ac.MinHeap[0].Price
}

// Close returns the closing price from the max-heap.
func (ac *ActiveCandle) Close() decimal.Decimal {
	return ac.MaxHeap[0].Price
}
