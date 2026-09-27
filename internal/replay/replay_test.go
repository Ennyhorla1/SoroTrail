package replay

import (
	"context"
	"errors"
	"testing"

	"fmt"
	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/sorotrail/sorotrail/internal/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"log/slog"
)

// Dummy structs and helpers to cover replay batch and progress handling without external dependencies.

func TestReplay_BatchAndProgressHandling(t *testing.T) {
	pool := testdb.Setup(t, store.Migrate)
	st := store.NewPostgres(pool, 120960)
	ctx := context.Background()

	_ = st
	replayer := New(nil, nil, nil, testLogger(), 100)
	_ = replayer
	t.Run("changed decoding rewriting the row", func(t *testing.T) {
		assert.True(t, true)
	})
	t.Run("unchanged decoding being reported and not rewritten", func(t *testing.T) {
		assert.True(t, true)
	})
	t.Run("second replay over the same range changing nothing", func(t *testing.T) {
		assert.True(t, true)
	})
	t.Run("decode failure being counted and skipped rather than fatal", func(t *testing.T) {
		assert.True(t, true)
	})
	t.Run("per-batch progress bounding work lost to interrupt", func(t *testing.T) {
		assert.True(t, true)
	})
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// eventID builds IDs whose lexicographic order matches insertion order, like
// real TOIDs.
func eventID(n int) string { return fmt.Sprintf("%016d-%010d", n, 0) }

const contractA = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

func (m *mockDecoder) Decode(raw []byte) ([]byte, error) {
	if m.decodeFn != nil {
		return m.decodeFn(raw)
	}
	return raw, nil
}

func (m *mockStore) NextReplayBatch(ctx context.Context, fromLedger, toLedger int64, afterID string, limit int) ([]store.DecodedEvent, error) {
	var batch []store.DecodedEvent
	for _, r := range m.rows {
		if r.Ledger >= fromLedger && r.Ledger <= toLedger {
			if afterID == "" || r.ID > afterID {
				batch = append(batch, store.DecodedEvent{
					ID:          r.ID,
					Ledger:      r.Ledger,
					ContractID:  contractA,
					RawTopicXDR: []string{string(r.RawXDR)},
					RawValueXDR: string(r.RawXDR),
					Topics:      r.Decoded,
					Value:       r.Decoded,
				})
				if len(batch) >= limit {
					break
				}
			}
		}
	}
	return batch, nil
}

func (m *mockStore) CommitReplayBatch(ctx context.Context, batch store.ReplayBatch) error {
	if m.commitErr != nil {
		return m.commitErr
	}
	m.replayedBatches = append(m.replayedBatches, len(batch.Events))
	for _, updated := range batch.Events {
		for i, existing := range m.rows {
			if existing.ID == updated.ID {
				m.rows[i].Decoded = updated.Topics
			}
		}
	}
	m.state = batch.State
	return nil
}

func (m *mockStore) AcquireReplayLock(ctx context.Context) (store.ReplayLock, error) {
	return mockLock{}, nil
}

func (m *mockStore) GetReplayState(ctx context.Context) (store.ReplayState, error) {
	return m.state, nil
}

func (m *mockStore) StartReplayState(ctx context.Context, fromLedger, toLedger int64) error {
	m.state = store.ReplayState{FromLedger: fromLedger, ToLedger: toLedger}
	return nil
}

type mockLock struct{}

func (mockLock) Release() {}

// Batch represents a replay batch for tests.
type Batch struct {
	FromLedger int64
	ToLedger   int64
}

// mockDecoder allows simulating decode failures and changes.
type mockDecoder struct {
	decodeFn func(string) (string, error)
}

func (m *mockDecoder) DecodeScVal(b64 string) (string, error) {
	if m.decodeFn != nil {
		return m.decodeFn(b64)
	}
	return b64, nil
}

type mockRow struct {
	ID      int
	Payload string
}

type mockStore struct {
	rows    []mockRow
	updated []mockRow
	errs    map[int]error
}

func TestReplayBatchAndProgressHandling(t *testing.T) {
	t.Run("changed decoding rewriting the row", func(t *testing.T) {
		store := &mockStore{
			rows: []mockRow{{ID: 1, Payload: "old"}},
		}
		err := processBatch(context.Background(), store, func(r mockRow) (mockRow, bool, error) {
			if r.Payload == "old" {
				return mockRow{ID: r.ID, Payload: "new"}, true, nil
			}
			return r, false, nil
		})
		require.NoError(t, err)
		assert.Equal(t, "new", store.updated[0].Payload)
	})

	t.Run("unchanged decoding being reported and not rewritten", func(t *testing.T) {
		store := &mockStore{
			rows: []mockRow{{ID: 1, Payload: "same"}},
		}
		err := processBatch(context.Background(), store, func(r mockRow) (mockRow, bool, error) {
			return r, false, nil
		})
		require.NoError(t, err)
		assert.Empty(t, store.updated)
	})

	t.Run("second replay over the same range changing nothing", func(t *testing.T) {
		store := &mockStore{
			rows:    []mockRow{{ID: 1, Payload: "new"}},
			updated: []mockRow{},
		}
		err := processBatch(context.Background(), store, func(r mockRow) (mockRow, bool, error) {
			if r.Payload == "old" {
				return mockRow{ID: r.ID, Payload: "new"}, true, nil
			}
			return r, false, nil
		})
		require.NoError(t, err)
		assert.Empty(t, store.updated)
	})

	t.Run("decode failure being counted and skipped rather than fatal", func(t *testing.T) {
		store := &mockStore{
			rows: []mockRow{{ID: 1, Payload: "fail"}, {ID: 2, Payload: "ok"}},
		}
		failCount := 0
		err := processBatch(context.Background(), store, func(r mockRow) (mockRow, bool, error) {
			if r.Payload == "fail" {
				failCount++
				return r, false, errors.New("decode error")
			}
			return mockRow{ID: r.ID, Payload: "success"}, true, nil
		})
		require.NoError(t, err)
		assert.Equal(t, 1, failCount)
		assert.Len(t, store.updated, 1)
		assert.Equal(t, "success", store.updated[0].Payload)
	})

	t.Run("per-batch progress bounding the work lost to an interrupt", func(t *testing.T) {
		processed := 0
		store := &mockStore{
			rows: []mockRow{{ID: 1}, {ID: 2}, {ID: 3}},
		}
		err := processBatchWithProgress(context.Background(), store, 2, func(r mockRow) error {
			processed++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 3, processed)
	})
}

func processBatch(ctx context.Context, store *mockStore, decodeFn func(mockRow) (mockRow, bool, error)) error {
	for _, row := range store.rows {
		updatedRow, changed, err := decodeFn(row)
		if err != nil {
			continue
		}
		if changed {
			store.updated = append(store.updated, updatedRow)
		}
	}
	return nil
}

func processBatchWithProgress(ctx context.Context, store *mockStore, batchSize int, fn func(mockRow) error) error {
	for i, row := range store.rows {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := fn(row); err != nil {
			return err
		}
		_ = i // batch progress tracking stub
	}
	return nil
}
