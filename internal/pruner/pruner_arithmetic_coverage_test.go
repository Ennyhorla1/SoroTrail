package pruner

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockArithmeticStore struct {
	store.Store
	mu                sync.Mutex
	deleteFunc        func(ctx context.Context, maxLedger uint32, maxAgeSeconds int64, batchSize int) (int64, error)
	deleteCalledCount int
	deletedLedgers    []uint32
	deletedAgeSeconds []int64
	deletedBatchSizes []int
}

func (m *mockArithmeticStore) Lock()   { m.mu.Lock() }
func (m *mockArithmeticStore) Unlock() { m.mu.Unlock() }

func (m *mockArithmeticStore) GetIngestionState(ctx context.Context) (store.IngestionState, error) {
	return store.IngestionState{Network: "default", LastIngestedLedger: 2000}, nil
}

func (m *mockArithmeticStore) AggregateEvents(ctx context.Context, filter store.EventFilter, scope store.Scope) ([]store.AggregateBucket, error) {
	return nil, nil
}

func (m *mockArithmeticStore) DeleteEventsOld(ctx context.Context, maxLedger uint32, maxAgeSeconds int64, batchSize int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteCalledCount++
	m.deletedLedgers = append(m.deletedLedgers, maxLedger)
	m.deletedAgeSeconds = append(m.deletedAgeSeconds, maxAgeSeconds)
	m.deletedBatchSizes = append(m.deletedBatchSizes, batchSize)
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, maxLedger, maxAgeSeconds, batchSize)
	}
	return 0, nil
}

func TestPrunerDeletionArithmeticCoverage(t *testing.T) {
	logger := slog.Default()

	t.Run("disabled pruner deletes nothing", func(t *testing.T) {
		st := &mockArithmeticStore{}
		p := New(st, logger, Options{MinLedger: 0, MaxAge: 0, BatchSize: 100})
		err := p.Run(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 0, st.deleteCalledCount)
	})

	t.Run("age-based and ledger-floor bounds alone and combined", func(t *testing.T) {
		tests := []struct {
			name           string
			opts           Options
			expectedLedger uint32
			expectedAge    int64
		}{
			{
				name:           "ledger only",
				opts:           Options{MinLedger: 500, BatchSize: 50},
				expectedLedger: 500,
				expectedAge:    0,
			},
			{
				name:           "age only",
				opts:           Options{MaxAge: 3600 * time.Second, BatchSize: 50},
				expectedLedger: 0,
				expectedAge:    3600,
			},
			{
				name:           "both combined",
				opts:           Options{MinLedger: 500, MaxAge: 3600 * time.Second, BatchSize: 50},
				expectedLedger: 500,
				expectedAge:    3600,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				st := &mockArithmeticStore{}
				p := New(st, logger, tc.opts)
				err := p.Run(context.Background())
				require.NoError(t, err)
				require.Equal(t, 1, st.deleteCalledCount)
				assert.Equal(t, tc.expectedLedger, st.deletedLedgers[0])
				assert.Equal(t, tc.expectedAge, st.deletedAgeSeconds[0])
			})
		}
	})

	t.Run("batching stops at configured size and resumes correctly", func(t *testing.T) {
		calls := 0
		st := &mockArithmeticStore{
			deleteFunc: func(ctx context.Context, maxLedger uint32, maxAgeSeconds int64, batchSize int) (int64, error) {
				calls++
				if calls < 3 {
					return int64(batchSize), nil
				}
				return 0, nil
			},
		}

		p := New(st, logger, Options{MinLedger: 1000, BatchSize: 42})
		err := p.Run(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 3, st.deleteCalledCount)
		for _, size := range st.deletedBatchSizes {
			assert.Equal(t, 42, size)
		}
	})

	t.Run("reported counts matching what was removed", func(t *testing.T) {
		st := &mockArithmeticStore{
			deleteFunc: func(ctx context.Context, maxLedger uint32, maxAgeSeconds int64, batchSize int) (int64, error) {
				if st.deleteCalledCount == 1 {
					return 15, nil
				}
				return 0, nil
			},
		}

		p := New(st, logger, Options{MinLedger: 100, BatchSize: 10})
		count, err := p.RunCount(context.Background())
		require.NoError(t, err)
		assert.Equal(t, int64(15), count)
	})

	t.Run("partial failure does not leave half-committed run", func(t *testing.T) {
		st := &mockArithmeticStore{
			deleteFunc: func(ctx context.Context, maxLedger uint32, maxAgeSeconds int64, batchSize int) (int64, error) {
				if st.deleteCalledCount == 2 {
					return 0, errors.New("database timeout")
				}
				return 10, nil
			},
		}

		p := New(st, logger, Options{MinLedger: 100, BatchSize: 10})
		err := p.Run(context.Background())
		require.Error(t, err)
		assert.Equal(t, 2, st.deleteCalledCount)
	})
}
