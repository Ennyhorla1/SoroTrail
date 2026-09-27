package replay

import (
	"context"
	"testing"

	"encoding/json"
	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/stretchr/testify/assert"
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

type mockLock struct {
	released bool
}

func (m *mockLock) Release() error {
	m.released = true
	return nil
}

func TestReplayLockInterface(t *testing.T) {
	var l store.ReplayLock = &mockLock{}
	err := l.Release()
	assert.NoError(t, err)
}
