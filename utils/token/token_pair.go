package token

type Symbol string

const (
	BTC_USDT Symbol = "btcusdt"
	ETH_USDT Symbol = "ethusdt"
	SOL_USDT Symbol = "solusdt"
)

func (s Symbol) String() string {
	return string(s)
}
