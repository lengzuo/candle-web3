package dto

import (
	"hermeneutic/utils/token"
	"time"

	"github.com/shopspring/decimal"
)

type Trade struct {
	InstrumentPair token.Symbol    `json:"instrument_pair"`
	Price          decimal.Decimal `json:"price"`
	Quantity       decimal.Decimal `json:"quantity"`
	Timestamp      time.Time       `json:"timestamp"`
}
