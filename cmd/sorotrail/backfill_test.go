package main

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunBackfill_Flags(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		expectError bool
	}{
		{"invalid contract", []string{"--contract", "invalid"}, true},
		{"missing ledger", []string{"--contract", "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4"}, true},
		{"inverted range", []string{"--contract", "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4", "--from-ledger", "100", "--to-ledger", "50"}, true},
		{"invalid batch size", []string{"--contract", "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4", "--from-ledger", "1", "--batch-size", "999"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := runBackfill(tt.args)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}