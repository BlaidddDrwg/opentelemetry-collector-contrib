// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	replay "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/recordreplay"
)

func TestRecordingDuration(t *testing.T) {
	for _, tt := range []struct {
		name   string
		config map[string]any
		want   time.Duration
		error  bool
	}{
		{name: "omitted defaults to five minutes", want: 5 * time.Minute},
		{name: "empty section defaults", config: map[string]any{"record": map[string]any{}}, want: 5 * time.Minute},
		{name: "configured", config: map[string]any{"record": map[string]any{"duration": "30s"}}, want: 30 * time.Second},
		{name: "zero", config: map[string]any{"record": map[string]any{"duration": "0s"}}, error: true},
		{name: "negative", config: map[string]any{"record": map[string]any{"duration": "-1s"}}, error: true},
		{name: "malformed", config: map[string]any{"record": map[string]any{"duration": "forever"}}, error: true},
		{name: "unknown field", config: map[string]any{"record": map[string]any{"duraton": "1s"}}, error: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			duration, err := recordingDuration(confmap.NewFromStringMap(tt.config))
			if tt.error {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, duration)
			}
		})
	}
}

func TestCaptureTimer(t *testing.T) {
	var output progressBuffer
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stop := startCaptureTimer(ctx, 10*time.Millisecond, &output, cancel)
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("capture duration did not request shutdown")
	}
	require.NoError(t, stop())
	require.NoError(t, stop(), "cleanup is idempotent")
	assert.Contains(t, output.String(), "Recording duration 10ms reached:")

	for _, cancelFirst := range []bool{false, true} {
		ctx, cancel = context.WithCancel(t.Context())
		var untouched bytes.Buffer
		stop = startCaptureTimer(ctx, time.Hour, &untouched, cancel)
		if cancelFirst {
			cancel()
		}
		require.NoError(t, stop(), "early shutdown must not wait for capture duration")
		cancel()
		assert.Empty(t, untouched.String())
	}
}

func TestTimedRecordingFlushesFiles(t *testing.T) {
	for _, mode := range []string{"fresh", "append", "idle"} {
		t.Run(mode, func(t *testing.T) {
			appendExisting := mode == "append"
			var lc net.ListenConfig
			listener, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			endpoint := listener.Addr().String()
			require.NoError(t, listener.Close())
			dir := t.TempDir()
			logPath := filepath.Join(dir, "logs.pb")
			logPayload := replayTestPayload(t, "logs", 0)
			freshInput := strings.NewReader("q")
			if appendExisting {
				var original bytes.Buffer
				require.NoError(t, replay.WriteHeader(&original))
				require.NoError(t, replay.WriteFrame(&original, replay.Record{TimestampUnixNano: 1, Payload: logPayload}))
				require.NoError(t, os.WriteFile(logPath, original.Bytes(), 0o600))
			}
			ready := make(chan struct{})
			var once sync.Once
			cmd := newCommand(zap.Hooks(func(entry zapcore.Entry) error {
				if entry.Message == "Everything is ready. Begin running and processing data." {
					once.Do(func() { close(ready) })
				}
				return nil
			}))
			args := []string{
				"record", "--config", "config.yaml", "--progress-interval=0",
				"--set", "record.duration=1s",
				"--set", "receivers.otlp.protocols.grpc.endpoint=" + endpoint,
				"--set", "receivers.otlp.protocols.http.endpoint=127.0.0.1:0",
			}
			for _, signal := range []string{"logs", "metrics", "traces"} {
				args = append(args,
					"--set", "exporters.file/"+signal+".path="+strconv.Quote(filepath.Join(dir, signal+".pb")),
					"--set", "exporters.file/"+signal+".flush_interval=1h",
					"--set", "exporters.file/"+signal+".sync_interval=1h")
			}
			input, confirmation := io.Pipe()
			defer func() { assert.NoError(t, input.Close()) }()
			defer func() { assert.NoError(t, confirmation.Close()) }()
			if appendExisting {
				args = append(args, "--append")
				cmd.SetIn(input)
			} else {
				cmd.SetIn(freshInput)
			}
			var output progressBuffer
			cmd.SetArgs(args)
			cmd.SetOut(&output)
			cmd.SetErr(&output)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- cmd.ExecuteContext(ctx) }()
			if appendExisting {
				require.Eventually(t, func() bool { return strings.Contains(output.String(), "Type yes") }, 5*time.Second, time.Millisecond)
				select {
				case err = <-done:
					t.Fatalf("recorder exited during confirmation: %v", err)
				case <-time.After(1100 * time.Millisecond):
				}
				assert.NotContains(t, output.String(), "Recording started:", "confirmation time must not consume capture duration")
				_, err = confirmation.Write([]byte("yes\n"))
				require.NoError(t, err)
			}
			select {
			case <-ready:
			case err = <-done:
				t.Fatalf("recorder exited before becoming ready: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("recorder did not become ready")
			}
			conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			defer func() { assert.NoError(t, conn.Close()) }()
			sendCtx, sendCancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer sendCancel()
			signals := []string{"logs", "metrics", "traces"}
			if mode == "idle" {
				signals = nil
			}
			for _, signal := range signals {
				payload := replayTestPayload(t, signal, 0)
				switch signal {
				case "logs":
					req := plogotlp.NewExportRequest()
					require.NoError(t, req.UnmarshalProto(payload))
					_, err = plogotlp.NewGRPCClient(conn).Export(sendCtx, req)
				case "metrics":
					req := pmetricotlp.NewExportRequest()
					require.NoError(t, req.UnmarshalProto(payload))
					_, err = pmetricotlp.NewGRPCClient(conn).Export(sendCtx, req)
				case "traces":
					req := ptraceotlp.NewExportRequest()
					require.NoError(t, req.UnmarshalProto(payload))
					_, err = ptraceotlp.NewGRPCClient(conn).Export(sendCtx, req)
				}
				require.NoError(t, err)
			}
			select {
			case err = <-done:
				require.NoError(t, err, "duration expiry must exit normally, not with deadline failure")
			case <-time.After(5 * time.Second):
				t.Fatal("capture did not stop at configured duration")
			}
			assert.Contains(t, output.String(), "duration=1s")
			assert.Contains(t, output.String(), "Recording duration 1s reached:")
			assert.Contains(t, output.String(), "Recording: status=complete")
			assert.Equal(t, 1, freshInput.Len(), "capture control must not read keypresses or change terminal input")
			for _, signal := range []string{"logs", "metrics", "traces"} {
				path := filepath.Join(dir, signal+".pb")
				file, size, openErr := replay.OpenInput(path)
				require.NoError(t, openErr)
				result, inspectErr := replay.Inspect(t.Context(), file, size, signal, 1024)
				require.NoError(t, inspectErr)
				require.NoError(t, file.Close())
				expected := uint64(1)
				if mode == "idle" {
					expected = 0
				}
				if appendExisting && signal == "logs" {
					expected++
				}
				assert.Equal(t, expected, result.Batches, "complete buffered batches must be flushed before exit")
			}
		})
	}
}
