package pruner

import (
	"context"
	"errors"
	"testing"

	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"time"
)

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

type mockArithmeticStore struct {
	store.Store
	prunedCount   int
	pruneErr      error
	lastBatchSize int
	lastMaxLedger uint64
	lastOlderThan int64
	ledgers       []uint64
	ages          []int64
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
	t.Run("disabled pruner deletes nothing", func(t *testing.T) {
		st := &mockArithmeticStore{prunedCount: 10}
		cfg := Config{
			Enabled:   false,
			BatchSize: 100,
		}
		p := New(st, cfg)
		err := p.Run(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 0, st.lastBatchSize)
	})

	t.Run("age based and ledger floor bounds", func(t *testing.T) {
		st := &mockArithmeticStore{prunedCount: 5}
		cfg := Config{
			Enabled:       true,
			MaxAgeSeconds: 3600,
			MinLedger:     1000,
			BatchSize:     50,
		}
		p := New(st, cfg)
		err := p.Run(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 50, st.lastBatchSize)
		assert.Equal(t, uint64(1000), st.lastMaxLedger)
		assert.NotZero(t, st.lastOlderThan)
	})

	t.Run("reported counts match removed", func(t *testing.T) {
		st := &mockArithmeticStore{prunedCount: 42}
		cfg := Config{
			Enabled:   true,
			BatchSize: 100,
		}
		p := New(st, cfg)
		err := p.Run(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 42, st.prunedCount)
	})

	t.Run("partial failure does not commit half run", func(t *testing.T) {
		st := &mockArithmeticStore{pruneErr: errors.New("db failure")}
		cfg := Config{
			Enabled:   true,
			BatchSize: 100,
		}
		p := New(st, cfg)
		err := p.Run(context.Background())
		require.Error(t, err)
	})
}
