package ratelimit

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// levelCapturingHandler records the level of every log record it receives,
// so a test can assert on log severity without depending on message text.
type levelCapturingHandler struct {
	levels []slog.Level
}

func (h *levelCapturingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *levelCapturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.levels = append(h.levels, r.Level)
	return nil
}
func (h *levelCapturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *levelCapturingHandler) WithGroup(string) slog.Handler      { return h }

// TestRecordRedisError_LogsAtWarnNotError is a regression test: background
// write/sync failures against Redis are an expected, self-healing degraded
// state for this backend (local counting keeps working) — not an
// operator-actionable emergency — so they must log at Warn, not Error. The
// metric (unaffected by log level) is the real alerting signal.
func TestRecordRedisError_LogsAtWarnNotError(t *testing.T) {
	handler := &levelCapturingHandler{}
	h := &HybridBackend{log: slog.New(handler)}

	h.recordRedisError("hybrid_sync", errors.New("boom"))

	require.Len(t, handler.levels, 1)
	assert.Equal(t, slog.LevelWarn, handler.levels[0])
	assert.NotEqual(t, slog.LevelError, handler.levels[0])
}

// TestRecordRedisError_NilMetricsDoesNotPanic confirms recordRedisError stays
// safe when constructed without a *monitoring.Metrics (as some tests do).
func TestRecordRedisError_NilMetricsDoesNotPanic(t *testing.T) {
	h := &HybridBackend{log: slog.New(&levelCapturingHandler{})}
	assert.NotPanics(t, func() {
		h.recordRedisError("hybrid_write", errors.New("boom"))
	})
}

// A failed sync round must not wipe the remote estimate: batchCurrentStatsErr
// reports failed keys as zero, and writing them made every instance admit up
// to the full limit on its own while Redis was failing.
func TestApplySync_KeepsRemoteEstimateOnRedisError(t *testing.T) {
	h := &HybridBackend{remoteStats: make(map[string][2]int)}
	key := "m:grant:claude-opus-4.6"
	keys := []string{key}
	t0 := time.Now()

	h.applySync(keys, syncResult{
		total: map[string][2]int{key: {7, 700}},
		local: map[string][2]int{key: {2, 200}},
	}, t0)
	rpm, tpm := h.remoteFor(key)
	require.Equal(t, 5, rpm)
	require.Equal(t, 500, tpm)

	// Redis error: failed keys come back as zero, the estimate must stay.
	h.applySync(keys, syncResult{
		total:    map[string][2]int{key: {0, 0}},
		local:    map[string][2]int{key: {2, 200}},
		totalErr: errors.New("redis: connection refused"),
	}, t0.Add(10*time.Second))
	rpm, _ = h.remoteFor(key)
	assert.Equal(t, 5, rpm, "a failed sync must keep the last known remote estimate")
	assert.Equal(t, 2, h.effectiveRPMLimit(key, 7))

	// Still failing after the RPM window: the estimate no longer describes the window.
	h.applySync(keys, syncResult{totalErr: context.DeadlineExceeded}, t0.Add(rpmWindow+time.Second))
	rpm, tpm = h.remoteFor(key)
	assert.Equal(t, 0, rpm)
	assert.Equal(t, 0, tpm)

	// Redis is back: the estimate is refreshed again.
	h.applySync(keys, syncResult{
		total: map[string][2]int{key: {4, 400}},
		local: map[string][2]int{key: {1, 100}},
	}, t0.Add(rpmWindow+2*time.Second))
	rpm, _ = h.remoteFor(key)
	assert.Equal(t, 3, rpm)
}
