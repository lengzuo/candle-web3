package binanceconnector

import "hermeneutic/utils/tokenpair"

type TokenPair string

const (
	BTC_USDT TokenPair = "BTC-USDT"
	ETH_USDT TokenPair = "ETH-USDT"
	SOL_USDT TokenPair = "SOL-USDT"
)

func (t TokenPair) String() string {
	return string(t)
}

var mapInternal = map[string]string{
	BTC_USDT.String(): tokenpair.BTC_USDT,
	ETH_USDT.String(): tokenpair.ETH_USDT,
	SOL_USDT.String(): tokenpair.SOL_USDT,
}

func ToInternal(token string) string {
	if _, ok := mapInternal[token]; !ok {
		return ""
	}
	return mapInternal[token]
}
