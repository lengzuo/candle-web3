package krakenconnector

import "hermeneutic/utils/token"

type Pair string

const (
	BTC_USDT Pair = "BTC/USDT"
	ETH_USDT Pair = "ETH/USDT"
	SOL_USDT Pair = "SOL/USDT"
)

func (t Pair) String() string {
	return string(t)
}

var mapSymbol = map[string]token.Symbol{
	BTC_USDT.String(): token.BTC_USDT,
	ETH_USDT.String(): token.ETH_USDT,
	SOL_USDT.String(): token.SOL_USDT,
}

func ToSymbol(token string) token.Symbol {
	if _, ok := mapSymbol[token]; !ok {
		return ""
	}
	return mapSymbol[token]
}
