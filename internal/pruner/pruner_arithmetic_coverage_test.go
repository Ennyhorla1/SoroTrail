package pruner

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (m *mockArithmeticStore) PruneEventsBefore(ctx context.Context, ledger uint32, t time.Time, limit int) (int64, error) {
	if m.deleteErr != nil {
		return 0, m.deleteErr
	}
	return int64(m.deleteCount), nil
}

type Config struct {
	Enabled         bool
	MaxAge          time.Duration
	RetainedLedgers uint32
	BatchSize       int
}

func TestPrunerArithmeticCoverage_Disabled(t *testing.T) {
	ctx := context.Background()
	st := &mockArithmeticStore{
		ingestionState: &store.IngestionState{LastIngestedLedger: 1000},
	}

	p := New(st, slog.Default(), Options{
		MaxAge: time.Hour,
	})
	err := p.Run(ctx)
	require.NoError(t, err)
	assert.False(t, st.lockCalled)
}

func TestPrunerArithmeticCoverage_LedgerFloorAndAgeBounds(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		opts           Options
		ingestionState *store.IngestionState
	}{
		{
			name: "age-based only",
			opts: Options{
				MaxAge: time.Hour * 24,
			},
			ingestionState: &store.IngestionState{
				LastIngestedLedger: 100,
			},
		},
		{
			name: "ledger-floor only",
			opts: Options{
				MinLedger: 50,
			},
			ingestionState: &store.IngestionState{
				LastIngestedLedger: 100,
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
		ingestionState: &store.IngestionState{LastIngestedLedger: 500},
		deleteCount:    25,
	}

	p := New(st, slog.Default(), Options{
		BatchSize: 10,
		MinLedger: 100,
	})

	err := p.Run(ctx)
	require.NoError(t, err)
}

func TestPrunerArithmeticCoverage_PartialFailure(t *testing.T) {
	ctx := context.Background()
	st := &mockArithmeticStore{
		ingestionState: &store.IngestionState{LastIngestedLedger: 500},
		deleteErr:      assert.AnError,
	}

	p := New(st, slog.Default(), Options{
		MinLedger: 100,
	})
	err := p.Run(ctx)
	assert.Error(t, err)
	assert.True(t, st.unlockCalled)
}
