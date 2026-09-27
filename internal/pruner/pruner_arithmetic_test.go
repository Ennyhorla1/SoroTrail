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

func (m *mockArithmeticStore) DeleteEventsBefore(ctx context.Context, maxLedger int64, beforeTime time.Time, limit int) (int64, error) {
	m.Lock()
	defer m.Unlock()
	m.deleteCalls++

	if m.deleteErr != nil {
		return 0, m.deleteErr
	}

	var matched []string
	for id, ev := range m.events {
		matchLedger := ev.Ledger < maxLedger
		matchTime := true
		if !beforeTime.IsZero() {
			matchTime = ev.CreatedAt.Before(beforeTime)
		}
		if matchLedger && matchTime {
			matched = append(matched, id)
			if len(matched) >= limit {
				break
			}
		}
	}

	for _, id := range matched {
		delete(m.events, id)
	}

	return int64(len(matched)), nil
}

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

func (m *mockArithmeticStore) DeleteOldEvents(ctx context.Context, maxLedger uint32, maxAgeSeconds int64, batchSize int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteCalledCount++
	m.deletedLedgers = append(m.deletedLedgers, maxLedger)
	m.deletedAgeSeconds = append(m.deletedAgeSeconds, maxAgeSeconds)
	m.deletedBatchSizes = append(m.deletedBatchSizes, batchSize)
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, maxLedger, maxAgeSeconds, batchSize)
	}
	return 10, nil
}

func (m *mockArithmeticStore) PruneEvents(ctx context.Context, olderThan int64, maxLedger uint64, batchSize int) (int, error) {
	return 0, nil
}

func (m *mockArithmeticStore) AggregateEvents(ctx context.Context, filter store.EventFilter, scope store.Scope) ([]any, error) {
	return nil, nil
}

// Unused store interface stubs to satisfy store.Store interface
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

func (m *mockArithmeticStore) CountEventsBefore(context.Context, int64, time.Time) (int64, error) {
	return 0, nil
}

func TestPrunerDeletionArithmetic(t *testing.T) {
	logger := slog.Default()

	t.Run("disabled pruner deletes nothing", func(t *testing.T) {
		st := &mockArithmeticStore{}
		p := New(st, logger, Options{MinLedger: 0, MaxAge: 0, BatchSize: 100})
		err := p.Run(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 0, st.deleteCalledCount)
	})

	cmb := &mockArithmeticStore{}
	_ = cmb

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
