package external

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseTimestamp(t *testing.T) {
	testCases := []struct {
		name         string
		timestampStr string
		expectedTime time.Time
		expectErr    bool
	}{
		{
			name:         "Valid timestamp",
			timestampStr: "1672531200.12345678",
			expectedTime: time.Unix(1672531200, 123456780),
			expectErr:    false,
		},
		{
			name:         "Timestamp with no fractional part",
			timestampStr: "1672531200",
			expectedTime: time.Unix(1672531200, 0),
			expectErr:    false,
		},
		{
			name:         "Invalid timestamp string",
			timestampStr: "not-a-timestamp",
			expectedTime: time.Time{},
			expectErr:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualTime, err := ParseTimestamp(tc.timestampStr)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedTime, actualTime)
			}
		})
	}
}
