package external

import (
	"context"
	"testing"
)

func TestConnector_handleMessage(t *testing.T) {
	msg := `{"channel":"trade","type":"update","data":[{"symbol":"BTC/USDT","side":"buy","price":118832.9,"qty":0.00409283,"ord_type":"market","trade_id":9622656,"timestamp":"2025-07-23T04:41:46.919844Z"},{"symbol":"BTC/USDT","side":"buy","price":118837.0,"qty":0.00033315,"ord_type":"market","trade_id":9622657,"timestamp":"2025-07-23T04:41:46.919844Z"},{"symbol":"BTC/USDT","side":"buy","price":118837.5,"qty":0.07963402,"ord_type":"market","trade_id":9622658,"timestamp":"2025-07-23T04:41:46.919844Z"}]}`
	c := &Connector{}
	c.handleMessage(context.Background(), []byte(msg))
}
