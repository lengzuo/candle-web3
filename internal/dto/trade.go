package dto

import (
	"time"

	"github.com/shopspring/decimal"
)

type Trade struct {
	InstrumentPair string
	Price          decimal.Decimal
	Quantity       decimal.Decimal
	Timestamp      time.Time
	TradeID        int64
}
