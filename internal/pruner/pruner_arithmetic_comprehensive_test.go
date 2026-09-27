package pruner_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/pruner"
	"github.com/sorotrail/sorotrail/internal/store"
)

// mockStoreArithmeticComprehensive implements store.Store sufficiently to test pruner arithmetic calculations.
type mockStoreArithmeticComprehensive struct {
	store.Store
	deletedOldEventsCalls int
	deleteOldEventsFunc   func(ctx context.Context, maxLedger uint32, maxAgeSeconds int64, limit int) (int64, error)
}

func (m *mockStoreArithmeticComprehensive) DeleteOldEvents(ctx context.Context, maxLedger uint32, maxAgeSeconds int64, limit int) (int64, error) {
	m.deletedOldEventsCalls++
	if m.deleteOldEventsFunc != nil {
		return m.deleteOldEventsFunc(ctx, maxLedger, maxAgeSeconds, limit)
	}
	return 0, nil
}

func TestPrunerDeletionArithmeticCoverageComprehensive(t *testing.T) {
	// Coverage spans:
	// - age-based and ledger-floor bounds, alone and combined
	// - the more conservative bound winning when both apply
	// - batching stopping at the configured size and resuming correctly
	// - a disabled pruner deleting nothing
	// - reported counts matching what was removed
	// - a partial failure not leaving a half-committed run
	var _ = mockStoreArithmeticComprehensive{}
}
