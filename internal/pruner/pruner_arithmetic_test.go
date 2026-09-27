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
	mu             sync.Mutex
	prunedCount    int
	pruneErr       error
	deleteErr      error
	deleteCalls    int
	lastBatchSize  int
	lastMaxLedger  uint64
	lastOlderThan  int64
	ingestedLedger int64
	events         map[string]store.Event
}

func (m *mockArithmeticStore) Lock()   { m.mu.Lock() }
func (m *mockArithmeticStore) Unlock() { m.mu.Unlock() }

func (m *mockArithmeticStore) GetIngestionState(ctx context.Context) (store.IngestionState, error) {
	m.Lock()
	defer m.Unlock()
	if m.ingestedLedger == 0 && len(m.events) == 0 {
		return store.IngestionState{}, store.ErrNotFound
	}
	return store.IngestionState{
		Network:            "default",
		LastIngestedLedger: m.ingestedLedger,
	}, nil
}

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

func (m *mockArithmeticStore) PruneEvents(ctx context.Context, olderThan int64, maxLedger uint64, batchSize int) (int, error) {
	if m.pruneErr != nil {
		return 0, m.pruneErr
	}
	m.lastBatchSize = batchSize
	m.lastMaxLedger = maxLedger
	m.lastOlderThan = olderThan
	return m.prunedCount, nil
}

func (m *mockArithmeticStore) AggregateEvents(context.Context, store.EventFilter, store.Scope) ([]any, error) {
	return nil, nil
}

func TestPrunerArithmetic_BoundsAndBatching(t *testing.T) {
	logger := slog.Default()
	t.Run("disabled pruner deletes nothing", func(t *testing.T) {
		st := &mockArithmeticStore{prunedCount: 10}
		opts := Options{
			MaxAge:    0,
			MinLedger: 0,
			BatchSize: 100,
		}
		p := New(st, logger, opts)
		err := p.Run(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 0, st.lastBatchSize)
	})

	t.Run("age based and ledger floor bounds", func(t *testing.T) {
		st := &mockArithmeticStore{prunedCount: 5, ingestedLedger: 2000}
		opts := Options{
			MaxAge:    time.Hour,
			MinLedger: 1000,
			BatchSize: 50,
		}
		p := New(st, logger, opts)
		// Run a single pruneOnce directly for testing arithmetic behavior without looping intervals
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := p.pruneOnce(ctx)
		require.NoError(t, err)
		assert.Equal(t, 50, st.lastBatchSize)
		assert.Equal(t, uint64(1000), st.lastMaxLedger)
	})

	t.Run("reported counts match removed", func(t *testing.T) {
		st := &mockArithmeticStore{prunedCount: 42, ingestedLedger: 2000}
		opts := Options{
			MinLedger: 1000,
			BatchSize: 100,
		}
		p := New(st, logger, opts)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := p.pruneOnce(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(42), p.Metrics().TotalRowsPurged)
	})

	t.Run("partial failure does not commit half run", func(t *testing.T) {
		st := &mockArithmeticStore{deleteErr: errors.New("db failure"), ingestedLedger: 2000}
		opts := Options{
			MinLedger: 1000,
			BatchSize: 100,
		}
		p := New(st, logger, opts)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := p.pruneOnce(ctx)
		require.Error(t, err)
	})
}
