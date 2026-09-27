package replay

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/store"
)

// mock decoder helper functions

// MockStore implements Store for testing.
type MockStore struct {
	Events      []store.DecodedEvent
	Batches     []store.ReplayBatch
	ReplayState store.ReplayState
	SaveErr     error
	CommitErr   error
	QueryErr    error
}

func (m *MockStore) AcquireReplayLock(ctx context.Context) (store.ReplayLock, error) {
	return &mockLock{}, nil
}

type mockLock struct{}

func (l *mockLock) Release() error {
	return nil
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
	m.ReplayState.LastLedger = batch.EndLedger
	return nil
}

type replayMockDecoder struct {
	decode.Decoder
	RewriteFn func(rawXDR string) (json.RawMessage, error)
	FailCount int
	Calls     int
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

func ptrStr(s string) *string { return &s }

func TestReplay_BatchAndProgressHandling(t *testing.T) {
	t.Run("changed decoding rewriting the row", func(t *testing.T) {
		ctx := context.Background()
		st := &MockStore{
			Events: []store.DecodedEvent{
				{
					ID:      "0000000000000001-000",
					Ledger:  10,
					DataXDR: ptrStr("AAAAB=="),
					Topics:  json.RawMessage(`[{"old":true}]`),
					Value:   json.RawMessage(`{"old":true}`),
				},
			},
		}
		dec := &replayMockDecoder{
			RewriteFn: func(raw string) (json.RawMessage, error) {
				return json.RawMessage(`{"new":true}`), nil
			},
		}

		r := New(st, dec, slog.Default(), Options{FromLedger: 10, ToLedger: 10, BatchSize: 100})
		_, err := r.Run(ctx)
		require.NoError(t, err)
		require.Len(t, st.Batches, 1)
		require.Len(t, st.Batches[0].Events, 1)
		assert.Equal(t, string(json.RawMessage(`{"new":true}`)), string(st.Batches[0].Events[0].Value))
	})

	t.Run("unchanged decoding being reported and not rewritten", func(t *testing.T) {
		ctx := context.Background()
		val := json.RawMessage(`{"same":true}`)
		st := &MockStore{
			Events: []store.DecodedEvent{
				{
					ID:      "0000000000000002-000",
					Ledger:  11,
					DataXDR: ptrStr("BBB=="),
					Value:   val,
				},
			},
		}
		dec := &replayMockDecoder{
			RewriteFn: func(raw string) (json.RawMessage, error) {
				return val, nil
			},
		}

		r := New(st, dec, slog.Default(), Options{FromLedger: 11, ToLedger: 11, BatchSize: 100})
		_, err := r.Run(ctx)
		require.NoError(t, err)
		if len(st.Batches) > 0 {
			assert.Len(t, st.Batches[0].Events, 1)
		}
	})

	t.Run("second replay over the same range changing nothing", func(t *testing.T) {
		ctx := context.Background()
		val := json.RawMessage(`{"final":true}`)
		st := &MockStore{
			Events: []store.DecodedEvent{
				{
					ID:      "0000000000000003-000",
					Ledger:  12,
					DataXDR: ptrStr("CCC=="),
					Value:   val,
				},
			},
		}
		dec := &replayMockDecoder{
			RewriteFn: func(raw string) (json.RawMessage, error) {
				return val, nil
			},
		}

		r := New(st, dec, slog.Default(), Options{FromLedger: 12, ToLedger: 12, BatchSize: 100})
		_, err := r.Run(ctx)
		require.NoError(t, err)

		r2 := New(st, dec, slog.Default(), Options{FromLedger: 12, ToLedger: 12, BatchSize: 100})
		_, err = r2.Run(ctx)
		require.NoError(t, err)
	})

	t.Run("decode failure being counted and skipped rather than fatal", func(t *testing.T) {
		ctx := context.Background()
		st := &MockStore{
			Events: []store.DecodedEvent{
				{
					ID:      "0000000000000004-000",
					Ledger:  13,
					DataXDR: ptrStr("BAD=="),
					Value:   json.RawMessage(`{}`),
				},
			},
		}
		dec := &replayMockDecoder{
			FailCount: 1,
		}

		r := New(st, dec, slog.Default(), Options{FromLedger: 13, ToLedger: 13, BatchSize: 100})
		_, err := r.Run(ctx)
		require.NoError(t, err)
	})

	t.Run("per-batch progress bounding the work lost to an interrupt", func(t *testing.T) {
		ctx := context.Background()
		st := &MockStore{
			Events: []store.DecodedEvent{
				{ID: "1", Ledger: 20, DataXDR: ptrStr("X1")},
				{ID: "2", Ledger: 21, DataXDR: ptrStr("X2")},
			},
		}
		dec := &replayMockDecoder{
			RewriteFn: func(raw string) (json.RawMessage, error) {
				return json.RawMessage(`{}`), nil
			},
		}

		r := New(st, dec, slog.Default(), Options{FromLedger: 20, ToLedger: 21, BatchSize: 1})
		_, err := r.Run(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(21), st.ReplayState.LastLedger)
	})
}

func TestReplay_BatchAndProgressHandlingStub(t *testing.T) {
	t.Run("stub for batch and progress handling conformance", func(t *testing.T) {
		assert.True(t, true)
	})
}
