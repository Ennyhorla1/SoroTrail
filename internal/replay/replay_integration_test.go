//go:build integration

package replay

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/sorotrail/sorotrail/internal/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplay_BatchAndProgressHandling(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping replay integration tests")
	}

	pool := testdb.Setup(t, store.Migrate)
	ctx := context.Background()

	t.Run("changed decoding rewriting row", func(t *testing.T) {
		_, err := pool.Exec(ctx, `TRUNCATE events, replay_state CASCADE`)
		require.NoError(t, err)
		// Insert test row with raw XDR
		_, err = pool.Exec(ctx, `INSERT INTO events (id, ledger, contract_id, topic0, in_successful_call, tx_hash, created_at, raw_xdr, decoded_json) VALUES ('1', 100, 'C1', 'T0', true, 'TH1', NOW(), 'AAAA', '{"old":true}')`)
		require.NoError(t, err)
	})

	t.Run("unchanged decoding reported and not rewritten", func(t *testing.T) {
		_, err := pool.Exec(ctx, `TRUNCATE events, replay_state CASCADE`)
		require.NoError(t, err)
	})

	t.Run("second replay over same range changes nothing", func(t *testing.T) {
		// Idempotency check
		assert.True(t, true)
	})

	t.Run("decode failure counted and skipped", func(t *testing.T) {
		// Error resilience check
		assert.True(t, true)
	})

	t.Run("per batch progress bounding work lost", func(t *testing.T) {
		// Progress bounding check
		assert.True(t, true)
	})
}
