package replay

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MockStore struct {
	store.ReplayStore
	ReplayState store.ReplayState
	Events      []store.DecodedEvent
	Batches     []store.ReplayBatch
	QueryErr    error
	CommitErr   error
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

type replayMockDecoder struct {
	Calls     int
	FailCount int
	RewriteFn func(string) (json.RawMessage, error)
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

type mockLock struct {
	released bool
}

func (m *mockLock) Release(ctx context.Context) error {
	m.released = true
	return nil
}

type mockStore struct {
	store.ReplayStore
	locks     []store.ReplayLock
	lockErr   error
	readRows  []store.ReplayRow
	readErr   error
	writeRows []store.ReplayRow
	writeErr  error
}

func (m *mockStore) AcquireReplayLock(ctx context.Context, name string) (store.ReplayLock, error) {
	if m.lockErr != nil {
		return nil, m.lockErr
	}
	l := &mockLock{}
	m.locks = append(m.locks, l)
	return l, nil
}

func (m *mockStore) ReadReplayBatch(ctx context.Context, cursor uint64, limit int) ([]store.ReplayRow, error) {
	if m.readErr != nil {
		return nil, m.readErr
	}
	if int(cursor) >= len(m.readRows) {
		return nil, nil
	}
	end := int(cursor) + limit
	if end > len(m.readRows) {
		end = len(m.readRows)
	}
	return m.readRows[cursor:end], nil
}

func (m *mockStore) WriteReplayBatch(ctx context.Context, rows []store.ReplayRow) error {
	if m.writeErr != nil {
		return m.writeErr
	}
	m.writeRows = append(m.writeRows, rows...)
	return nil
}

type mockDecoder struct {
	decodeFn func(data []byte) ([]byte, error)
}

func (d *mockDecoder) Decode(data []byte) ([]byte, error) {
	if d.decodeFn != nil {
		return d.decodeFn(data)
	}
	return data, nil
}

func TestReplayLockInterface(t *testing.T) {
	var l store.ReplayLock = &mockLock{}
	err := l.Release(context.Background())
	assert.NoError(t, err)
}

func TestReplayBatchAndProgressHandling(t *testing.T) {
	var processed int64
	ev1 := seedEvent(1, 100)
	ev2 := seedEvent(2, 101)
	ms := newFakeStore(ev1, ev2)
	dec := improvedDecoder()
	r := New(ms, dec, testLogger(), Options{
		FromLedger: 1,
		ToLedger:   1000,
		BatchSize:  1,
		Progress: func(n int64) {
			processed += n
		},
	})
	sum, err := r.Run(context.Background())
	require.NoError(t, err)
	assert.True(t, sum.Completed)
	assert.Equal(t, int64(2), processed)
}
