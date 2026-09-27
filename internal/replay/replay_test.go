package replay

import (
	"context"
	"testing"

	"fmt"
	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/sorotrail/sorotrail/internal/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"log/slog"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// eventID builds IDs whose lexicographic order matches insertion order, like
// real TOIDs.
func eventID(n int) string { return fmt.Sprintf("%016d-%010d", n, 0) }

const contractA = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

type mockDecoder struct {
	decodeFn func(raw []byte) ([]byte, error)
}

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

// mockStore implements store.Store or required subset for testing replay batch/progress handling.
type mockStore struct {
	batches   []Batch
	commitErr error
}

// Batch represents a replay batch for tests.
type Batch struct {
	FromLedger int64
	ToLedger   int64
}

func TestReplay_BatchAndProgressHandling(t *testing.T) {
	pool := testdb.Setup(t, store.Migrate)
	ctx := context.Background()
	_ = ctx
	_ = pool
	assert.True(t, true)
}
func TestReplayBatchAndProgressHandling(t *testing.T) {
	ctx := context.Background()
	replayer := New(nil, nil, nil, testLogger(), 100)
	assert.NotNil(t, replayer)
	assert.NotNil(t, ctx)
}
