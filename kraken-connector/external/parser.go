package external

import (
	"time"

	"github.com/shopspring/decimal"
)

// ParseTimestamp converts a decimal string representation of a Unix timestamp
// into a time.Time object.
func ParseTimestamp(timestampStr string) (time.Time, error) {
	ts, err := decimal.NewFromString(timestampStr)
	if err != nil {
		return time.Time{}, err
	}
	sec := ts.IntPart()
	nsec := ts.Sub(decimal.NewFromInt(sec)).Mul(decimal.NewFromInt(1e9)).IntPart()
	return time.Unix(sec, nsec), nil
}
