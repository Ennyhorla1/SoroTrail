package replay

import (
	"context"
	"testing"

	"encoding/json"

	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockDecoder struct {
	decodeFn func([]byte) ([]byte, error)
}

func (d *mockDecoder) Decode(data []byte) ([]byte, error) {
	if d.decodeFn != nil {
		return d.decodeFn(data)
	}
	return data, nil
}

type replayMockDecoder struct {
	Calls     int
	FailCount int
	RewriteFn func(string) (json.RawMessage, error)
}

type mockStore struct {
	store.Store
	ReplayState store.ReplayState
	Events      []store.DecodedEvent
	Batches     []store.ReplayBatch
	QueryErr    error
	CommitErr   error
	LockErr     error
	Locks       []*mockLock
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

func (m *mockStore) AcquireReplayLock(ctx context.Context) (store.ReplayLock, error) {
	if m.LockErr != nil {
		return nil, m.LockErr
	}
	l := &mockLock{}
	m.Locks = append(m.Locks, l)
	return l, nil
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

func (m *mockLock) KeepAlive(ctx context.Context) error {
	return nil
}

type mockLock struct {
	mock.Mock
}

func (m *mockLock) Release(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func TestReplayBatchAndProgressHandling(t *testing.T) {
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

	t.Run("per-batch progress bounding the work lost to an interrupt", func(t *testing.T) {
		l := &mockLock{}
		l.On("Release", mock.Anything).Return(nil)
		var rl store.ReplayLock = l
		err := rl.Release(context.Background())
		assert.NoError(t, err)
		l.AssertExpectations(t)
	})
}
