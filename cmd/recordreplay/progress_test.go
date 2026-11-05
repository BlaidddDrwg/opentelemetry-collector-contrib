// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	replay "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/recordreplay"
)

type progressBuffer struct {
	mu   sync.Mutex
	data bytes.Buffer
}

func (b *progressBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.data.Write(data)
}

func (b *progressBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.data.String()
}

func TestRecordingProgressIdleAndConcurrentWrites(t *testing.T) {
	var output progressBuffer
	progress := newSessionProgress("Recording", &output, time.Millisecond)
	progress.start("files=test.pb")
	require.Eventually(t, func() bool {
		return strings.Contains(output.String(), "state=waiting_for_data batches=0")
	}, time.Second, time.Millisecond)
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			for range 10 {
				progress.recorded(42)
			}
		})
	}
	wg.Wait()
	require.Eventually(t, func() bool {
		return strings.Contains(output.String(), "state=recording batches=30 payload_bytes=1260")
	}, time.Second, time.Millisecond)
	require.NoError(t, progress.finish(nil))
	final := output.String()
	assert.Contains(t, final, "Recording: status=complete")
	assert.Contains(t, final, "last_success_age=")
	// finish waits for the reporting goroutine, so output cannot grow afterwards.
	assert.Equal(t, final, output.String())
}

func TestProgressDisabledAndNotStarted(t *testing.T) {
	var output bytes.Buffer
	progress := newSessionProgress("Recording", &output, 0)
	require.NoError(t, progress.finish(nil))
	assert.Empty(t, output.String(), "validation and cancelled preflight must remain silent")
	progress.start("files=test.pb")
	progress.recorded(12)
	require.NoError(t, progress.finish(nil))
	assert.Contains(t, output.String(), "Recording started:")
	assert.Contains(t, output.String(), "batches=1 payload_bytes=12")
	assert.NotContains(t, output.String(), "Recording progress")
}

func TestReplayProgressDuringWaitAndSend(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/x-protobuf")
		if calls.Add(1) > 1 {
			<-r.Context().Done()
		}
	}))
	t.Cleanup(server.Close)
	cfg := replay.DefaultConfig()
	cfg.Signal, cfg.Protocol, cfg.Endpoint, cfg.TLS.Insecure = "logs", "http", server.URL, true
	cfg.Timeout, cfg.LatenessTolerance = 2*time.Second, time.Second
	cfg.File = filepath.Join(t.TempDir(), "input.pb")
	payload := replayTestPayload(t, "logs", 0)
	data := recordingTestFile(t,
		replay.Record{TimestampUnixNano: int64(time.Second), Payload: payload},
		replay.Record{TimestampUnixNano: int64(time.Second + 200*time.Millisecond), Payload: payload},
	)
	require.NoError(t, os.WriteFile(cfg.File, data, 0o400))
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	var output progressBuffer
	done := make(chan error, 1)
	go func() { done <- replayFile(ctx, cfg, &output, &output, 10*time.Millisecond) }()
	require.Eventually(t, func() bool {
		text := output.String()
		return strings.Contains(text, "Replay progress: sent=1") && strings.Contains(text, "state=waiting")
	}, time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		return strings.Contains(output.String(), "state=sending")
	}, time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("progress reporting prevented replay shutdown")
	}
	text := output.String()
	assert.Contains(t, text, "endpoint="+strconv.Quote(server.URL))
	assert.Contains(t, text, "payload event timestamps are unchanged")
	assert.Contains(t, text, "record=2 recorded_time=1970-01-01T00:00:01.2Z offset=200ms wait_remaining=")
	final := text[strings.LastIndex(text, "\nReplay:")+1:]
	assert.Contains(t, final, "sent=1 backwards=0")
	assert.Contains(t, final, "payload_bytes="+strconv.Itoa(len(payload)))
	assert.Contains(t, final, "status=stopped")
	after, err := os.ReadFile(cfg.File)
	require.NoError(t, err)
	assert.Equal(t, data, after)
}
