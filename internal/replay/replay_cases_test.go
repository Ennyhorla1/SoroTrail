package replay

import (
	"context"
	"testing"

	"encoding/json"
	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (d *mockDecoder) Decode(data []byte) ([]byte, error) {
	if d.decodeFn != nil {
		return d.decodeFn(data)
	}
	return data, nil
}

type mockStore struct {
	ReplayState store.ReplayState
	Events      []store.DecodedEvent
	Batches     []store.ReplayBatch
	QueryErr    error
	CommitErr   error
	LockErr     error
	Locks       []*mockLock
}

type mockLock struct{}

func (l *mockLock) Release() error {
	return nil
}

func (m *mockStore) GetReplayState(ctx context.Context) (store.ReplayState, error) {
	return m.ReplayState, nil
}

func (m *mockStore) StartReplayState(ctx context.Context, fromLedger, toLedger int64) error {
	m.ReplayState = store.ReplayState{
		FromLedger: fromLedger,
		ToLedger:   toLedger,
	}
	return nil
}

func (m *mockStore) NextReplayBatch(ctx context.Context, fromLedger, toLedger int64, afterID string, limit int) ([]store.DecodedEvent, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	var res []store.DecodedEvent
	for _, ev := range m.Events {
		if ev.Ledger >= fromLedger && ev.Ledger <= toLedger {
			if afterID == "" || ev.ID > afterID {
				res = append(res, ev)
				if len(res) >= limit {
					break
				}
			}
		}
	}
	return res, nil
}

func (m *mockStore) CommitReplayBatch(ctx context.Context, batch store.ReplayBatch) error {
	if m.CommitErr != nil {
		return m.CommitErr
	}
	m.Batches = append(m.Batches, batch)
	for i, ev := range m.Events {
		for _, be := range batch.Events {
			if ev.ID == be.ID {
				m.Events[i].Topics = be.Topics
				m.Events[i].Value = be.Value
			}
		}
	}
	m.ReplayState = batch.State
	return nil
}

func (m *mockStore) AcquireReplayLock(ctx context.Context, name string) (store.ReplayLock, error) {
	if m.LockErr != nil {
		return nil, m.LockErr
	}
	l := &mockLock{}
	m.Locks = append(m.Locks, l)
	return l, nil
}

type replayMockDecoder struct {
	Calls     int
	FailCount int
	RewriteFn func(rawXDR string) (json.RawMessage, error)
}

func (md *replayMockDecoder) DecodeScVal(rawXDR string) (json.RawMessage, error) {
	md.Calls++
	if md.FailCount > 0 && md.Calls <= md.FailCount {
		return nil, assert.AnError
	}
	if md.RewriteFn != nil {
		return md.RewriteFn(rawXDR)
	}
	return json.RawMessage(`{}`), nil
}

func TestReplay_BatchAndProgress(t *testing.T) {
	const contractA = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	makeEvent := func(id string, ledger int64, topic, val string) store.DecodedEvent {
		return store.DecodedEvent{
			ID:          id,
			ContractID:  contractA,
			Ledger:      ledger,
			RawTopicXDR: []string{topic},
			RawValueXDR: val,
			Topics:      json.RawMessage(`["old-topic"]`),
			Value:       json.RawMessage(`"old-value"`),
		}
	}

	t.Run("changed decoding rewriting row", func(t *testing.T) {
		ms := &mockStore{
			Events: []store.DecodedEvent{makeEvent("001", 10, "t1", "v1")},
		}
		dec := &replayMockDecoder{
			RewriteFn: func(raw string) (json.RawMessage, error) {
				return json.RawMessage(`"new-value"`), nil
			},
		}
		r := New(ms, dec, testLogger(), Options{FromLedger: 1, ToLedger: 100, BatchSize: 10})
		sum, err := r.Run(context.Background())
		require.NoError(t, err)
		assert.EqualValues(t, 1, sum.Changed)
		assert.Equal(t, `"new-value"`, string(ms.Events[0].Value))
	})

	t.Run("unchanged decoding reported and not rewritten", func(t *testing.T) {
		ms := &mockStore{
			Events: []store.DecodedEvent{makeEvent("001", 10, "t1", "v1")},
		}
		dec := &replayMockDecoder{
			RewriteFn: func(raw string) (json.RawMessage, error) {
				return json.RawMessage(`"old-value"`), nil
			},
		}
		r := New(ms, dec, testLogger(), Options{FromLedger: 1, ToLedger: 100, BatchSize: 10})
		sum, err := r.Run(context.Background())
		require.NoError(t, err)
		assert.EqualValues(t, 0, sum.Changed)
		assert.EqualValues(t, 1, sum.Processed)
	})

	t.Run("second replay over same range changes nothing", func(t *testing.T) {
		ms := &mockStore{
			Events: []store.DecodedEvent{makeEvent("001", 10, "t1", "v1")},
		}
		dec := &replayMockDecoder{
			RewriteFn: func(raw string) (json.RawMessage, error) {
				return json.RawMessage(`"new-value"`), nil
			},
		}
		r := New(ms, dec, testLogger(), Options{FromLedger: 1, ToLedger: 100, BatchSize: 10})
		_, err := r.Run(context.Background())
		require.NoError(t, err)

		// Second run
		sum2, err := r.Run(context.Background())
		require.NoError(t, err)
		assert.EqualValues(t, 0, sum2.Changed)
		assert.EqualValues(t, 1, sum2.Processed)
	})

	t.Run("decode failure counted and skipped rather than fatal", func(t *testing.T) {
		ms := &mockStore{
			Events: []store.DecodedEvent{makeEvent("001", 10, "t1", "v1")},
		}
		dec := &replayMockDecoder{
			FailCount: 1,
		}
		r := New(ms, dec, testLogger(), Options{FromLedger: 1, ToLedger: 100, BatchSize: 10})
		sum, err := r.Run(context.Background())
		require.NoError(t, err)
		assert.EqualValues(t, 1, sum.Failed)
		assert.EqualValues(t, 1, sum.Processed)
		assert.EqualValues(t, 0, sum.Changed)
	})

	t.Run("per-batch progress bounding work lost", func(t *testing.T) {
		var progressCalls int64
		ms := &mockStore{
			Events: []store.DecodedEvent{
				makeEvent("001", 10, "t1", "v1"),
				makeEvent("002", 11, "t2", "v2"),
			},
		}
		dec := &replayMockDecoder{
			RewriteFn: func(raw string) (json.RawMessage, error) {
				return json.RawMessage(`"new"`), nil
			},
		}
		r := New(ms, dec, testLogger(), Options{
			FromLedger: 1,
			ToLedger:   100,
			BatchSize:  1,
			Progress: func(processed int64) {
				progressCalls += processed
			},
		})
		sum, err := r.Run(context.Background())
		require.NoError(t, err)
		assert.EqualValues(t, 2, sum.Processed)
		assert.EqualValues(t, 2, progressCalls)
	})
}
