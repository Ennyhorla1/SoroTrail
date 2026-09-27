package pruner

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (m *mockArithmeticStore) DeleteOldEvents(ctx context.Context, maxLedger uint32, maxAgeSeconds int64, batchSize int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteCalledCount++
	m.deletedLedgers = append(m.deletedLedgers, int64(maxLedger))
	m.deletedAgeSeconds = append(m.deletedAgeSeconds, maxAgeSeconds)
	m.deletedBatchSizes = append(m.deletedBatchSizes, batchSize)
	if m.deleteErr != nil {
		return 0, m.deleteErr
	}
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, int64(maxLedger), time.Unix(maxAgeSeconds, 0), batchSize)
	}
	return 10, nil
}

func (m *mockArithmeticStore) UpsertEvents(context.Context, []store.Event) (int64, error) {
	return 0, nil
}

func (m *mockArithmeticStore) GetEvent(context.Context, string, store.Scope) (store.Event, error) {
	return store.Event{}, store.ErrNotFound
}

func (m *mockArithmeticStore) QueryEvents(context.Context, store.EventFilter, store.Scope) ([]store.Event, error) {
	return nil, nil
}

func (m *mockArithmeticStore) AddWatchedContract(context.Context, string) error { return nil }

func (m *mockArithmeticStore) RemoveWatchedContract(context.Context, string) error { return nil }

func (m *mockArithmeticStore) ListWatchedContracts(context.Context) ([]string, error) {
	return nil, nil
}

func (m *mockArithmeticStore) Stats(context.Context, store.Scope) (store.Stats, error) {
	return store.Stats{}, nil
}

func (m *mockArithmeticStore) GetReplayState(context.Context) (store.ReplayState, error) {
	return store.ReplayState{}, store.ErrNotFound
}

func (m *mockArithmeticStore) ReplaceEventsInRange(context.Context, int64, int64, []store.Event) error {
	return nil
}

func (m *mockArithmeticStore) SaveAuditState(context.Context, store.AuditState) error { return nil }

func (m *mockArithmeticStore) GetAuditState(context.Context) (store.AuditState, error) {
	return store.AuditState{}, store.ErrNotFound
}

func (m *mockArithmeticStore) Ping(context.Context) error { return nil }

func (m *mockArithmeticStore) CountContracts(context.Context, store.ContractsFilter) (int64, error) {
	return 0, nil
}

func (m *mockArithmeticStore) AggregateEvents(ctx context.Context, filter store.EventFilter, scope string) ([]store.AggregateBucket, error) {
	return nil, nil
}

type mockArithmeticStore struct {
	store.Store
	mu                sync.Mutex
	lockCalled        bool
	unlockCalled      bool
	ingestionState    *store.IngestionState
	ingestionErr      error
	deleteFunc        func(ctx context.Context, maxLedger int64, beforeTime time.Time, batchSize int) (int64, error)
	deleteCalledCount int
	deletedLedgers    []int64
	deletedBefore     []time.Time
	deletedBatchSizes []int
	deletedAgeSeconds []int64
	deleteErr         error
	deleteCalls       int
	deleteCount       int
	Events            map[string]store.Event
}

func (m *mockArithmeticStore) Lock() {
	m.mu.Lock()
	m.lockCalled = true
}

func (m *mockArithmeticStore) Unlock() {
	m.mu.Unlock()
	m.unlockCalled = true
}

func (m *mockArithmeticStore) GetIngestionState(ctx context.Context) (store.IngestionState, error) {
	if m.ingestionState != nil {
		return *m.ingestionState, m.ingestionErr
	}
	return store.IngestionState{Network: "default", LastIngestedLedger: 2000}, m.ingestionErr
}

func (m *mockArithmeticStore) CountEventsBefore(ctx context.Context, maxLedger int64, beforeTime time.Time) (int64, error) {
	return int64(m.deleteCount), m.deleteErr
}

func (m *mockArithmeticStore) CountEvents(ctx context.Context, filter store.EventFilter) (int64, error) {
	return 0, nil
}

func (m *mockArithmeticStore) PruneEventsBefore(ctx context.Context, ledger uint32, t time.Time, limit int) (int64, error) {
	if m.deleteErr != nil {
		return 0, m.deleteErr
	}
	return int64(m.deleteCount), nil
}

func (m *mockArithmeticStore) LockPruner(ctx context.Context) (bool, error) {
	m.Lock()
	m.lockCalled = true
	return true, nil
}

func (m *mockArithmeticStore) UnlockPruner(ctx context.Context) error {
	m.Unlock()
	m.unlockCalled = true
	return nil
}

func (m *mockArithmeticStore) DeleteEventsBefore(ctx context.Context, maxLedger int64, beforeTime time.Time, limit int) (int64, error) {
	m.Lock()
	defer m.Unlock()
	m.deleteCalledCount++
	m.deletedLedgers = append(m.deletedLedgers, maxLedger)
	m.deletedBefore = append(m.deletedBefore, beforeTime)
	m.deletedBatchSizes = append(m.deletedBatchSizes, limit)

	if m.deleteErr != nil {
		return 0, m.deleteErr
	}
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, maxLedger, beforeTime, limit)
	}
	count := int64(m.deleteCount)
	if limit > 0 && count > int64(limit) {
		count = int64(limit)
	}
	return count, nil
}

func TestPrunerDeletionArithmetic(t *testing.T) {
	t.Run("disabled pruner deletes nothing", func(t *testing.T) {
		st := &mockArithmeticStore{}
		p := New(st, nil, Options{MinLedger: 0, MaxAge: 0, BatchSize: 100})
		err := p.Run(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 0, st.deleteCalledCount)
		assert.False(t, st.lockCalled)
	})

	t.Run("age-based and ledger-floor bounds alone and combined", func(t *testing.T) {
		tests := []struct {
			name           string
			opts           Options
			expectedLedger int64
			hasAge         bool
		}{
			{
				name:           "ledger only",
				opts:           Options{MinLedger: 500, BatchSize: 50},
				expectedLedger: 500,
				hasAge:         false,
			},
			{
				name:           "age only",
				opts:           Options{MaxAge: 3600 * time.Second, BatchSize: 50},
				expectedLedger: 2000,
				hasAge:         true,
			},
			{
				name:           "both combined",
				opts:           Options{MinLedger: 500, MaxAge: 3600 * time.Second, BatchSize: 50},
				expectedLedger: 500,
				hasAge:         true,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				st := &mockArithmeticStore{}
				p := New(st, nil, tc.opts)
				err := p.Run(context.Background())
				require.NoError(t, err)
				require.Equal(t, 1, st.deleteCalledCount)
				assert.Equal(t, tc.expectedLedger, st.deletedLedgers[0])
				if tc.hasAge {
					assert.False(t, st.deletedBefore[0].IsZero())
				} else {
					assert.True(t, st.deletedBefore[0].IsZero())
				}
			})
		}
	})

	t.Run("batching stops at configured size and resumes correctly", func(t *testing.T) {
		calls := 0
		st := &mockArithmeticStore{
			deleteFunc: func(ctx context.Context, maxLedger int64, beforeTime time.Time, batchSize int) (int64, error) {
				calls++
				if calls < 3 {
					return int64(batchSize), nil
				}
				return 0, nil
			},
		}

		p := New(st, nil, Options{MinLedger: 1000, BatchSize: 42})
		err := p.Run(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 3, st.deleteCalledCount)
		for _, size := range st.deletedBatchSizes {
			assert.Equal(t, 42, size)
		}
	})

	t.Run("reported counts matching what was removed", func(t *testing.T) {
		st := &mockArithmeticStore{
			deleteFunc: func(ctx context.Context, maxLedger int64, beforeTime time.Time, batchSize int) (int64, error) {
				if st.deleteCalledCount == 1 {
					return 15, nil
				}
				return 0, nil
			},
		}

		p := New(st, nil, Options{MinLedger: 100, BatchSize: 10})
		count, err := p.pruneOnce(context.Background())
		require.NoError(t, err)
		assert.Equal(t, int64(15), count)
	})

	t.Run("partial failure does not leave half-committed run", func(t *testing.T) {
		st := &mockArithmeticStore{
			deleteFunc: func(ctx context.Context, maxLedger int64, beforeTime time.Time, batchSize int) (int64, error) {
				if st.deleteCalledCount == 2 {
					return 0, errors.New("database timeout")
				}
				return 10, nil
			},
		}

		p := New(st, nil, Options{MinLedger: 100, BatchSize: 10})
		err := p.Run(context.Background())
		assert.Error(t, err)
		assert.True(t, st.unlockCalled)
	})
}
