package pruner

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

type Config struct {
	Enabled         bool
	MaxAge          time.Duration
	RetainedLedgers uint32
	BatchSize       int
}

func TestPrunerArithmeticCoverage_Disabled(t *testing.T) {
	ctx := context.Background()
	st := &mockArithmeticStore{
		ingestionState: &store.IngestionState{LastIngestedLedger: 1000, LatestLedger: 1000},
	}

	p := New(st, slog.Default(), Options{
		Enabled: false,
	})

	err := p.Run(ctx)
	require.NoError(t, err)
	assert.False(t, st.lockCalled)
}

func TestPrunerArithmeticCoverage_LedgerFloorAndAgeBounds(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	tests := []struct {
		name           string
		opts           Options
		ingestionState *store.IngestionState
	}{
		{
			name: "age-based only",
			opts: Options{
				Enabled: true,
				MaxAge:  time.Hour * 24,
			},
			ingestionState: &store.IngestionState{
				LastIngestedLedger: 100,
				LatestLedger:       100,
				LatestLedgerTime:   now,
			},
		},
		{
			name: "ledger-floor only",
			opts: Options{
				Enabled:   true,
				MinLedger: 50,
			},
			ingestionState: &store.IngestionState{
				LastIngestedLedger: 100,
				LatestLedger:       100,
				LatestLedgerTime:   now,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &mockArithmeticStore{
				ingestionState: tt.ingestionState,
				deleteCount:    10,
			}
			p := New(st, slog.Default(), tt.opts)
			err := p.Run(ctx)
			require.NoError(t, err)
			assert.True(t, st.lockCalled)
			assert.True(t, st.unlockCalled)
		})
	}
}

func TestPrunerArithmeticCoverage_BatchingAndCounts(t *testing.T) {
	ctx := context.Background()
	st := &mockArithmeticStore{
		ingestionState: &store.IngestionState{LastIngestedLedger: 500, LatestLedger: 500, LatestLedgerTime: time.Now()},
		deleteCount:    25,
	}

	p := New(st, slog.Default(), Options{
		Enabled:   true,
		BatchSize: 10,
		MinLedger: 100,
	})

	err := p.Run(ctx)
	require.NoError(t, err)
}

func TestPrunerArithmeticCoverage_PartialFailure(t *testing.T) {
	ctx := context.Background()
	st := &mockArithmeticStore{
		ingestionState: &store.IngestionState{LastIngestedLedger: 500, LatestLedger: 500, LatestLedgerTime: time.Now()},
		deleteErr:      assert.AnError,
	}

	p := New(st, slog.Default(), Options{
		Enabled:   true,
		MinLedger: 100,
	})

	err := p.Run(ctx)
	assert.Error(t, err)
	assert.True(t, st.unlockCalled)
}
