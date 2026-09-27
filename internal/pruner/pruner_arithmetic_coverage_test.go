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

type mockArithmeticStore struct {
	store.Store
	lockCalled       bool
	unlockCalled     bool
	ingestionState   *store.IngestionState
	ingestionErr     error
	aggregateBuckets []store.AggregateBucket
	aggregateErr     error
	deleteCount      int
	deleteErr        error
}

func (m *mockArithmeticStore) Lock() {
	m.lockCalled = true
}

func (m *mockArithmeticStore) Unlock() {
	m.unlockCalled = true
}

func (m *mockArithmeticStore) GetIngestionState(ctx context.Context) (store.IngestionState, error) {
	if m.ingestionState != nil {
		return *m.ingestionState, m.ingestionErr
	}
	return store.IngestionState{}, m.ingestionErr
}

func (m *mockArithmeticStore) AggregateEvents(ctx context.Context, filter store.EventFilter, scope store.Scope) ([]store.AggregateBucket, error) {
	return m.aggregateBuckets, m.aggregateErr
}

func (m *mockArithmeticStore) DeleteEventsBefore(ctx context.Context, maxLedger int64, maxAge time.Time, batchSize int) (int64, error) {
	if m.deleteErr != nil {
		return 0, m.deleteErr
	}
	count := int64(m.deleteCount)
	if batchSize > 0 && count > int64(batchSize) {
		count = int64(batchSize)
	}
	m.deleteCount -= int(count)
	return count, nil
}

func TestPrunerArithmetic_Disabled(t *testing.T) {
	ctx := context.Background()
	st := &mockArithmeticStore{
		ingestionState: &store.IngestionState{LatestLedger: 1000},
	}

	p := New(st, slog.Default(), Options{
		Enabled: false,
	})

	err := p.Run(ctx)
	require.NoError(t, err)
	assert.False(t, st.lockCalled)
}

func TestPrunerArithmetic_LedgerFloorAndAgeBounds(t *testing.T) {
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
				LatestLedger:     100,
				LatestLedgerTime: now,
			},
		},
		{
			name: "ledger-floor only",
			opts: Options{
				Enabled:   true,
				MinLedger: 50,
			},
			ingestionState: &store.IngestionState{
				LatestLedger:     100,
				LatestLedgerTime: now,
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

func TestPrunerArithmetic_BatchingAndCounts(t *testing.T) {
	ctx := context.Background()
	st := &mockArithmeticStore{
		ingestionState: &store.IngestionState{LatestLedger: 500, LatestLedgerTime: time.Now()},
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

func TestPrunerArithmetic_PartialFailure(t *testing.T) {
	ctx := context.Background()
	st := &mockArithmeticStore{
		ingestionState: &store.IngestionState{LatestLedger: 500, LatestLedgerTime: time.Now()},
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
