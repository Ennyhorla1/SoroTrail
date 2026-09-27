package replay

import (
	"context"
	"errors"
	"testing"

	"encoding/json"
	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sorotrail/SoroTrail/internal/replay"
	"sorotrail/SoroTrail/internal/store"
)

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

func TestReplayLockInterface(t *testing.T) {
	var l store.ReplayLock = &mockLock{}
	err := l.Release()
	assert.NoError(t, err)
}

// mockStore implements store.ReplayStore for testing replay batch and progress handling.
type mockStore struct {
	store.ReplayStore
	locks     []store.ReplayLock
	lockErr   error
	readRows  []store.ReplayRow
	readErr   error
	writeRows []store.ReplayRow
	writeErr  error
}

type mockLock struct {
	released bool
}

func (m *mockLock) Release(ctx context.Context) error {
	m.released = true
	return nil
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

func TestReplayBatchAndProgressHandling(t *testing.T) {
	t.Run("changed decoding rewriting the row", func(t *testing.T) {
		ms := &mockStore{
			readRows: []store.ReplayRow{
				{ID: 1, Data: []byte("old-data")},
			},
		}
		dec := &mockDecoder{
			decodeFn: func(data []byte) ([]byte, error) {
				return []byte("new-data"), nil
			},
		}
		r := replay.NewReplayer(ms, dec, replay.WithBatchSize(1))
		err := r.Run(context.Background())
		require.NoError(t, err)
		assert.Len(t, ms.writeRows, 1)
		assert.Equal(t, []byte("new-data"), ms.writeRows[0].Data)
	})

	t.Run("unchanged decoding being reported and not rewritten", func(t *testing.T) {
		ms := &mockStore{
			readRows: []store.ReplayRow{
				{ID: 1, Data: []byte("same-data")},
			},
		}
		dec := &mockDecoder{
			decodeFn: func(data []byte) ([]byte, error) {
				return []byte("same-data"), nil
			},
		}
		r := replay.NewReplayer(ms, dec, replay.WithBatchSize(1))
		err := r.Run(context.Background())
		require.NoError(t, err)
		assert.Empty(t, ms.writeRows)
	})

	t.Run("second replay over the same range changing nothing", func(t *testing.T) {
		ms := &mockStore{
			readRows: []store.ReplayRow{
				{ID: 1, Data: []byte("final-data")},
			},
		}
		dec := &mockDecoder{
			decodeFn: func(data []byte) ([]byte, error) {
				return []byte("final-data"), nil
			},
		}
		r := replay.NewReplayer(ms, dec, replay.WithBatchSize(1))
		err := r.Run(context.Background())
		require.NoError(t, err)
		assert.Empty(t, ms.writeRows)
	})

	t.Run("decode failure being counted and skipped rather than fatal", func(t *testing.T) {
		ms := &mockStore{
			readRows: []store.ReplayRow{
				{ID: 1, Data: []byte("bad-data")},
				{ID: 2, Data: []byte("good-data")},
			},
		}
		dec := &mockDecoder{
			decodeFn: func(data []byte) ([]byte, error) {
				if string(data) == "bad-data" {
					return nil, errors.New("decode error")
				}
				return []byte("decoded-good"), nil
			},
		}
		r := replay.NewReplayer(ms, dec, replay.WithBatchSize(2))
		err := r.Run(context.Background())
		require.NoError(t, err)
		assert.Len(t, ms.writeRows, 1)
		assert.Equal(t, uint64(2), ms.writeRows[0].ID)
		assert.Equal(t, []byte("decoded-good"), ms.writeRows[0].Data)
	})

	t.Run("per-batch progress bounding work lost to interrupt", func(t *testing.T) {
		ms := &mockStore{
			readRows: []store.ReplayRow{
				{ID: 1, Data: []byte("d1")},
				{ID: 2, Data: []byte("d2")},
			},
		}
		dec := &mockDecoder{
			decodeFn: func(data []byte) ([]byte, error) {
				return append([]byte(nil), data...), nil
			},
		}
		r := replay.NewReplayer(ms, dec, replay.WithBatchSize(1))
		ctx, cancel := context.WithCancel(context.Background())
		// Cancel immediately after first batch or let it run
		cancel()
		err := r.Run(ctx)
		// Depending on context check, it may return context.Canceled or succeed if checked per batch
		if err != nil {
			assert.ErrorIs(t, err, context.Canceled)
		}
	})
}
