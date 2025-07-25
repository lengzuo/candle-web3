package binanceconnector

import "hermeneutic/utils/token"

type Pair string

const (
	BTC_USDT Pair = "BTCUSDT"
	ETH_USDT Pair = "ETHUSDT"
	SOL_USDT Pair = "SOLUSDT"
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
