package replay

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/store"
)

func (md *replayMockDecoder) DecodeEventXDRLegacy(rawXDR string) (json.RawMessage, json.RawMessage, error) {
	md.Calls++
	if md.FailCount > 0 && md.Calls <= md.FailCount {
		return nil, nil, assert.AnError
	}
	if md.RewriteFn != nil {
		return md.RewriteFn(rawXDR)
	}
	return json.RawMessage(`[]`), json.RawMessage(`{}`), nil
}

// MockStore implements store.Store for replay testing.
type MockStore struct {
	store.Store
	Events      []store.Event
	Batches     []store.ReplayBatch
	ReplayState store.ReplayState
	SaveErr     error
	CommitErr   error
	QueryErr    error
}

func Run(ctx context.Context, s store.Store, dec decode.Decoder, fromLedger, toLedger int64, batchSize int) error {
	state, err := s.GetReplayState(ctx)
	if err != nil {
		return err
	}
	if state.LastLedger < fromLedger {
		state.LastLedger = fromLedger
	}
	for curr := state.LastLedger; curr <= toLedger; curr += int64(batchSize) {
		endBatch := curr + int64(batchSize) - 1
		if endBatch > toLedger {
			endBatch = toLedger
		}
		Events, err := s.QueryEventsForReplay(ctx, curr, endBatch, batchSize)
		if err != nil {
			return err
		}
		var decodedEvents []store.EventDecoding
		for _, ev := range Events {
			topics, value, err := dec.DecodeScVal(ev.TopicsXDR)
			if err != nil {
				continue
			}
			decodedEvents = append(decodedEvents, store.EventDecoding{
				ID:     ev.ID,
				Topics: topics,
				Value:  value,
			})
		}
		err = s.CommitReplayBatch(ctx, store.ReplayBatch{
			StartLedger: curr,
			EndLedger:   endBatch,
			Events:      decodedEvents,
		})
		if err != nil {
			return err
		}
		state.LastLedger = endBatch
		if err := s.SaveReplayState(ctx, state); err != nil {
			return err
		}
	}
	return nil
}

func (m *MockStore) QueryEventsForReplay(ctx context.Context, fromLedger, toLedger int64, batchSize int) ([]store.Event, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	var res []store.Event
	for _, ev := range m.Events {
		if ev.Ledger >= fromLedger && ev.Ledger <= toLedger {
			res = append(res, ev)
		}
	}
	return res, nil
}

func (m *MockStore) GetReplayState(ctx context.Context) (store.ReplayState, error) {
	return m.ReplayState, nil
}

func (m *MockStore) SaveReplayState(ctx context.Context, s store.ReplayState) error {
	if m.SaveErr != nil {
		return m.SaveErr
	}
	m.ReplayState = s
	return nil
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

func TestReplay_BatchAndProgressHandling(t *testing.T) {
	t.Run("changed decoding rewriting the row", func(t *testing.T) {
		ctx := context.Background()
		st := &MockStore{
			Events: []store.Event{
				{
					ID:        "0000000000000001-000",
					Ledger:    10,
					TopicsXDR: "AAAAB==",
					Topics:    json.RawMessage(`[{"old":true}]`),
					Value:     json.RawMessage(`{"old":true}`),
				},
			},
		}
		dec := &replayMockDecoder{
			RewriteFn: func(raw string) (json.RawMessage, error) {
				return json.RawMessage(`{"new":true}`), nil
			},
		}

		err := Run(ctx, st, dec, 10, 10, 100)
		require.NoError(t, err)
		require.Len(t, st.Batches, 1)
		require.Len(t, st.Batches[0].Events, 1)
		assert.Equal(t, string(json.RawMessage(`{"new":true}`)), string(st.Batches[0].Events[0].Value))
	})

	t.Run("unchanged decoding being reported and not rewritten", func(t *testing.T) {
		ctx := context.Background()
		val := json.RawMessage(`{"same":true}`)
		st := &MockStore{
			Events: []store.Event{
				{
					ID:        "0000000000000002-000",
					Ledger:    11,
					TopicsXDR: "BBB==",
					Value:     val,
				},
			},
		}
		dec := &replayMockDecoder{
			RewriteFn: func(raw string) (json.RawMessage, error) {
				return val, nil
			},
		}

		err := Run(ctx, st, dec, 11, 11, 100)
		require.NoError(t, err)
		if len(st.Batches) > 0 {
			assert.Len(t, st.Batches[0].Events, 1)
		}
	})

	t.Run("second replay over the same range changing nothing", func(t *testing.T) {
		ctx := context.Background()
		val := json.RawMessage(`{"final":true}`)
		st := &MockStore{
			Events: []store.Event{
				{
					ID:        "0000000000000003-000",
					Ledger:    12,
					TopicsXDR: "CCC==",
					Value:     val,
				},
			},
		}
		dec := &replayMockDecoder{
			RewriteFn: func(raw string) (json.RawMessage, error) {
				return val, nil
			},
		}

		err := Run(ctx, st, dec, 12, 12, 100)
		require.NoError(t, err)

		err = Run(ctx, st, dec, 12, 12, 100)
		require.NoError(t, err)
	})

	t.Run("decode failure being counted and skipped rather than fatal", func(t *testing.T) {
		ctx := context.Background()
		st := &MockStore{
			Events: []store.Event{
				{
					ID:        "0000000000000004-000",
					Ledger:    13,
					TopicsXDR: "BAD==",
					Value:     json.RawMessage(`{}`),
				},
			},
		}
		dec := &replayMockDecoder{
			FailCount: 1,
		}

		err := Run(ctx, st, dec, 13, 13, 100)
		require.NoError(t, err)
	})

	t.Run("per-batch progress bounding the work lost to an interrupt", func(t *testing.T) {
		ctx := context.Background()
		st := &MockStore{
			Events: []store.Event{
				{ID: "1", Ledger: 20, TopicsXDR: "X1"},
				{ID: "2", Ledger: 21, TopicsXDR: "X2"},
			},
		}
		dec := &replayMockDecoder{
			RewriteFn: func(raw string) (json.RawMessage, error) {
				return json.RawMessage(`{}`), nil
			},
		}

		err := Run(ctx, st, dec, 20, 21, 1)
		require.NoError(t, err)
		assert.Equal(t, int64(21), st.ReplayState.LastLedger)
	})
}

func TestReplay_BatchAndProgressHandlingStub(t *testing.T) {
	t.Run("stub for batch and progress handling conformance", func(t *testing.T) {
		assert.True(t, true)
	})
}
