package pruner

import (
	"context"
	"testing"
	"time"

	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type Config struct {
	Enabled         bool
	MaxAge          time.Duration
	RetainedLedgers uint32
	BatchSize       int
}
type mockArithmeticStore struct {
	mock.Mock
}

func (m *mockArithmeticStore) CountEventsBefore(ctx context.Context, ledger int64, t time.Time, limit int) (int64, error) {
	args := m.Called(ctx, ledger, t, limit)
	return args.Get(0).(int64), args.Error(1)
}

func (m *mockArithmeticStore) DeleteEventsBefore(ctx context.Context, ledger int64, t time.Time, limit int) (int64, error) {
	args := m.Called(ctx, ledger, t, limit)
	return args.Get(0).(int64), args.Error(1)
}

func TestPrunerArithmeticBoundsAndBatching(t *testing.T) {
	t.Run("disabled pruner deletes nothing", func(t *testing.T) {
		st := new(mockArithmeticStore)
		p := New(st, Config{Enabled: false})
		count, err := p.Prune(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, int64(0), count)
		st.AssertNotCalled(t, "DeleteEventsBefore", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("batching stops at configured size and resumes", func(t *testing.T) {
		st := new(mockArithmeticStore)
		st.On("CountEventsBefore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(int64(150), nil)
		st.On("DeleteEventsBefore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(int64(100), nil).Once()
		st.On("DeleteEventsBefore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(int64(50), nil).Once()

		p := New(st, Config{Enabled: true, BatchSize: 100})
		count, err := p.Prune(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, int64(150), count)
		st.AssertExpectations(t)
	})

	t.Run("conservative bound winning when both apply", func(t *testing.T) {
		st := new(mockArithmeticStore)
		st.On("CountEventsBefore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(int64(10), nil)
		st.On("DeleteEventsBefore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(int64(10), nil)

		p := New(st, Config{Enabled: true, MaxAge: time.Hour, MinLedger: 500})
		count, err := p.Prune(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, int64(10), count)
		st.AssertExpectations(t)
	})

	t.Run("partial failure stops execution safely", func(t *testing.T) {
		st := new(mockArithmeticStore)
		st.On("CountEventsBefore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(int64(100), nil)
		st.On("DeleteEventsBefore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(int64(0), assert.AnError)

		p := New(st, Config{Enabled: true, BatchSize: 100})
		count, err := p.Prune(context.Background())
		assert.Error(t, err)
		assert.Equal(t, int64(0), count)
		st.AssertExpectations(t)
	})
}
