package replay

import (
	"context"
	"testing"

	"encoding/json"
	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (m *MockStore) AcquireReplayLock(ctx context.Context) (store.ReplayLock, error) {
	return &mockLock{}, nil
}

func (m *MockStore) GetReplayState(ctx context.Context) (store.ReplayState, error) {
	return m.ReplayState, nil
}

func (m *MockStore) StartReplayState(ctx context.Context, fromLedger, toLedger int64) error {
	return nil
}

func (m *MockStore) NextReplayBatch(ctx context.Context, fromLedger, toLedger int64, afterID string, limit int) ([]store.DecodedEvent, error) {
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

func (m *MockStore) CommitReplayBatch(ctx context.Context, batch store.ReplayBatch) error {
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

type mockLock struct{}

type MockStore struct {
	Events      []store.DecodedEvent
	Batches     []store.ReplayBatch
	ReplayState store.ReplayState
	SaveErr     error
	CommitErr   error
	QueryErr    error
}

type replayMockDecoder struct {
	decode.Decoder
	RewriteFn func(rawXDR string) (json.RawMessage, error)
	FailCount int
	Calls     int
}

func (m *mockLock) Release() error {
	return nil
}

func TestReplayBatchAndProgressHandlingCases(t *testing.T) {
	t.Run("changed decoding rewriting row", func(t *testing.T) {
		ev := seedEvent(1, 100)
		st := &MockStore{Events: []store.DecodedEvent{ev}}
		dec := &replayMockDecoder{
			RewriteFn: func(rawXDR string) (json.RawMessage, error) {
				return json.RawMessage(`{"updated":true}`), nil
			},
		}
		r := New(st, dec, testLogger(), Options{FromLedger: 50, ToLedger: 150})
		sum, err := r.Run(context.Background())
		require.NoError(t, err)
		assert.EqualValues(t, 1, sum.Changed)
		assert.True(t, sum.Completed)
	})

	t.Run("unchanged decoding reported and not rewritten", func(t *testing.T) {
		ev := seedEvent(1, 100)
		st := &MockStore{Events: []store.DecodedEvent{ev}}
		dec := &replayMockDecoder{
			RewriteFn: func(rawXDR string) (json.RawMessage, error) {
				return ev.Value, nil
			},
		}
		r := New(st, dec, testLogger(), Options{FromLedger: 50, ToLedger: 150})
		sum, err := r.Run(context.Background())
		require.NoError(t, err)
		assert.EqualValues(t, 0, sum.Changed)
		assert.EqualValues(t, 1, sum.Processed)
	})

	t.Run("second replay over same range changing nothing", func(t *testing.T) {
		ev := seedEvent(1, 100)
		st := &MockStore{Events: []store.DecodedEvent{ev}}
		dec := &replayMockDecoder{}
		r1 := New(st, dec, testLogger(), Options{FromLedger: 50, ToLedger: 150})
		_, err := r1.Run(context.Background())
		require.NoError(t, err)
		r2 := New(st, dec, testLogger(), Options{FromLedger: 50, ToLedger: 150})
		sum2, err := r2.Run(context.Background())
		require.NoError(t, err)
		assert.EqualValues(t, 0, sum2.Changed)
		assert.True(t, sum2.Completed)
	})

	t.Run("decode failure counted and skipped rather than fatal", func(t *testing.T) {
		ev := seedEvent(1, 100)
		st := &MockStore{Events: []store.DecodedEvent{ev}}
		dec := &replayMockDecoder{FailCount: 1}
		r := New(st, dec, testLogger(), Options{FromLedger: 50, ToLedger: 150})
		sum, err := r.Run(context.Background())
		require.NoError(t, err)
		assert.EqualValues(t, 1, sum.Failed)
		assert.EqualValues(t, 1, sum.Processed)
		assert.True(t, sum.Completed)
	})

	t.Run("per-batch progress bounding work lost to interrupt", func(t *testing.T) {
		ev1 := seedEvent(1, 100)
		ev2 := seedEvent(2, 101)
		st := &MockStore{Events: []store.DecodedEvent{ev1, ev2}}
		dec := &replayMockDecoder{}
		var progressCalls int64
		opts := Options{
			FromLedger: 50,
			ToLedger:   150,
			BatchSize:  1,
			Progress: func(processed int64) {
				progressCalls += processed
			},
		}
		r := New(st, dec, testLogger(), opts)
		sum, err := r.Run(context.Background())
		require.NoError(t, err)
		assert.True(t, sum.Completed)
		assert.EqualValues(t, 2, progressCalls)
	})
}
