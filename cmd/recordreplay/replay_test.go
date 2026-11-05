// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	replay "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/recordreplay"
)

func TestReplayAbsoluteDeadlines(t *testing.T) {
	for _, tt := range []struct {
		name       string
		timestamps []int64
		sendDelay  time.Duration
		prepareLag time.Duration
		wakeLag    time.Duration
		wantTimes  []time.Duration
		wantBack   uint64
		wantErr    string
	}{
		{
			name:       "processing and response latency do not accumulate drift",
			timestamps: []int64{100, 200, 400}, sendDelay: 20 * time.Millisecond, prepareLag: 5 * time.Millisecond,
			wantTimes: []time.Duration{0, 100 * time.Millisecond, 300 * time.Millisecond},
		},
		{
			name:       "equal timestamps",
			timestamps: []int64{100, 100, 100},
			wantTimes:  []time.Duration{0, 0, 0},
		},
		{
			name:       "backwards immediate without resetting baseline",
			timestamps: []int64{100, 200, 150, 400},
			wantTimes:  []time.Duration{0, 100 * time.Millisecond, 100 * time.Millisecond, 300 * time.Millisecond},
			wantBack:   1,
		},
		{
			name:       "before first timestamp remains immediate",
			timestamps: []int64{100, 50, 75, 200},
			wantTimes:  []time.Duration{0, 0, 0, 100 * time.Millisecond},
			wantBack:   2,
		},
		{
			name:       "late response stops next send instead of shifting schedule",
			timestamps: []int64{100, 200, 300}, sendDelay: 120 * time.Millisecond,
			wantTimes: []time.Duration{0}, wantErr: "record 2 missed replay deadline",
		},
		{
			name:       "preparation lateness is included",
			timestamps: []int64{100, 200}, prepareLag: 120 * time.Millisecond,
			wantTimes: []time.Duration{0}, wantErr: "record 2 missed replay deadline",
		},
		{
			name:       "scheduler wakeup misses tolerance",
			timestamps: []int64{100, 200}, wakeLag: 11 * time.Millisecond,
			wantErr: "record 1 missed replay deadline",
		},
		{
			name:       "backwards is exempt from original deadline tolerance",
			timestamps: []int64{100, 200, 150}, sendDelay: 50 * time.Millisecond,
			wantTimes: []time.Duration{0, 100 * time.Millisecond, 150 * time.Millisecond}, wantBack: 1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := replay.DefaultConfig()
			cfg.Signal = "logs"
			now := time.Unix(1000, 0)
			var start time.Time
			var sentTimes []time.Duration
			var data bytes.Buffer
			records := make([]replay.Record, len(tt.timestamps))
			for i, timestamp := range tt.timestamps {
				records[i] = replay.Record{TimestampUnixNano: timestamp * int64(time.Millisecond), Payload: replayTestPayload(t, "logs", i)}
				if i > 0 {
					require.NoError(t, replay.WriteFrame(&data, records[i]))
				}
			}
			clock := replay.Clock{Now: func() time.Time { return now }, Wait: func(ctx context.Context, deadline time.Time) error {
				if deadline.After(now) {
					now = deadline
				}
				now = now.Add(tt.wakeLag)
				return ctx.Err()
			}}

			prepare := func(payload []byte, _ time.Duration) (func(context.Context) error, error) {
				now = now.Add(tt.prepareLag)
				if start.IsZero() {
					start = now
				}
				return func(context.Context) error {
					assert.Equal(t, replayTestPayload(t, "logs", len(sentTimes)), payload, "file order must be preserved")
					sentTimes = append(sentTimes, now.Sub(start))
					now = now.Add(tt.sendDelay)
					return nil
				}, nil
			}
			var warnings bytes.Buffer
			stats, err := replay.Replay(t.Context(), &data, records[0], cfg, prepare, clock, nil, &warnings, nil)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantTimes, sentTimes)
			assert.Equal(t, uint64(len(sentTimes)), stats.Sent)
			assert.Equal(t, tt.wantBack, stats.Backwards)
			assert.Equal(t, int(tt.wantBack), strings.Count(warnings.String(), "WARNING:"))
		})
	}
}

func TestReplaySchedulerFailures(t *testing.T) {
	sendErr := errors.New("destination failed")
	prepareErr := errors.New("invalid protobuf")
	for _, tt := range []struct {
		name    string
		first   int64
		next    []byte
		prepare error
		send    error
		want    string
		sent    uint64
	}{
		{name: "send", send: sendErr, want: "record 1 export failed"},
		{name: "prepare", prepare: prepareErr, want: "invalid logs payload"},
		{name: "truncated tail", next: []byte{0, 0}, want: "cannot read record 2", sent: 1},
		{name: "excessive length", next: []byte{255, 255, 255, 255}, want: "exceeds limit", sent: 1},
		{
			name: "overflow", first: math.MinInt64,
			next: []byte{0, 0, 0, 8, 127, 255, 255, 255, 255, 255, 255, 255},
			want: "offset exceeds time.Duration range", sent: 1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := replay.DefaultConfig()
			cfg.Signal = "logs"
			now := time.Now()
			clock := replay.Clock{Now: func() time.Time { return now }, Wait: func(context.Context, time.Time) error { return nil }}
			stats, err := replay.Replay(t.Context(), bytes.NewReader(tt.next), replay.Record{TimestampUnixNano: tt.first}, cfg,
				func([]byte, time.Duration) (func(context.Context) error, error) {
					return func(context.Context) error { return tt.send }, tt.prepare
				}, clock, nil, io.Discard, nil)
			require.ErrorContains(t, err, tt.want)
			assert.Equal(t, tt.sent, stats.Sent)
		})
	}
}

func TestReplayCancellationAndTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	cfg := replay.DefaultConfig()
	stats, err := replay.Replay(ctx, bytes.NewReader(nil), replay.Record{}, cfg,
		func([]byte, time.Duration) (func(context.Context) error, error) {
			t.Fatal("cancelled replay must not prepare or send")
			return nil, nil
		}, replay.Clock{}, nil, io.Discard, nil)
	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, stats.Sent)
	require.ErrorIs(t, replay.WaitDeadline(ctx, time.Now().Add(time.Hour)), context.Canceled)

	ctx, cancel = context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- replay.WaitDeadline(ctx, time.Now().Add(time.Hour)) }()
	cancel()
	select {
	case err = <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("long replay wait did not cancel promptly")
	}

	cfg.Timeout = time.Millisecond
	cfg.Signal = "logs"
	stats, err = replay.Replay(t.Context(), bytes.NewReader(nil), replay.Record{}, cfg,
		func([]byte, time.Duration) (func(context.Context) error, error) {
			return func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			}, nil
		}, replay.Clock{Now: time.Now, Wait: replay.WaitDeadline}, nil, io.Discard, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Zero(t, stats.Sent)
}

func TestReplayFileReadOnlyAndEmpty(t *testing.T) {
	cfg := replay.DefaultConfig()
	cfg.Signal = "logs"
	cfg.Endpoint = "127.0.0.1:1"
	for _, tt := range []struct {
		name string
		data []byte
		want string
	}{
		{name: "header only", data: recordingTestFile(t)},
		{name: "empty", want: "cannot read recording header"},
		{name: "legacy", data: []byte("legacy recording"), want: "not a versioned RecordReplay"},
		{name: "incomplete header", data: []byte("OTEL"), want: "cannot read recording header"},
		{name: "incomplete first record", data: append(recordingTestFile(t), 0, 0), want: "cannot read first recording record"},
		{
			name: "oversized first payload",
			data: append(recordingTestFile(t), 255, 255, 255, 255), want: "exceeds limit",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg.File = filepath.Join(t.TempDir(), "input.pb")
			require.NoError(t, os.WriteFile(cfg.File, tt.data, 0o400))
			var output bytes.Buffer
			err := replayFile(t.Context(), cfg, io.Discard, &output, defaultProgressInterval)
			if tt.want != "" {
				require.ErrorContains(t, err, tt.want)
			} else {
				require.NoError(t, err, "header-only replay must not connect to the unreachable endpoint")
				assert.Contains(t, output.String(), "0 records")
			}
			after, err := os.ReadFile(cfg.File)
			require.NoError(t, err)
			assert.True(t, bytes.Equal(tt.data, after), "replay must leave all input bytes unchanged")
		})
	}
}

func TestReplayConfiguration(t *testing.T) {
	t.Setenv("RR_TEST_ENDPOINT", "127.0.0.1:12345")
	cfg, err := loadReplayConfig(t.Context(), []string{"config.yaml"}, []string{
		"replay.endpoint=${env:RR_TEST_ENDPOINT}", "replay.lateness_tolerance=25ms",
		"replay.tls.insecure=true", "replay.signal=traces",
	}, "override.pb")
	require.NoError(t, err)
	assert.Equal(t, "override.pb", cfg.File)
	assert.Equal(t, "traces", cfg.Signal)
	assert.Equal(t, "127.0.0.1:12345", cfg.Endpoint)
	assert.Equal(t, 25*time.Millisecond, cfg.LatenessTolerance)
	assert.True(t, cfg.TLS.Insecure)

	for _, override := range []string{
		"replay.file=\"\"", "replay.signal=unknown", "replay.endpoint=\"\"", "replay.endpoint=https://localhost:4317",
		"replay.endpoint=localhost:bad", "replay.endpoint=localhost:0", "replay.endpoint=localhost:99999", "replay.endpoint=\":4317\"",
		"replay.protocol=udp", "replay.lateness_tolerance=-1s", "replay.timeout=0s",
		"replay.max_payload_size=0", "replay.max_payload_size=4294967295", "replay.unknown=1",
		"not-an-override", "exporters.file/logs.path=unrelated.pb",
	} {
		t.Run(override, func(t *testing.T) {
			_, loadErr := loadReplayConfig(t.Context(), []string{"config.yaml"}, []string{override}, "")
			require.Error(t, loadErr)
		})
	}
	for _, tt := range []struct {
		endpoint string
		insecure bool
		wantErr  bool
	}{
		{endpoint: "http://localhost:4318", insecure: true},
		{endpoint: "https://localhost:4318"},
		{endpoint: "http://localhost:4318", wantErr: true},
		{endpoint: "https://localhost:4318", insecure: true, wantErr: true},
		{endpoint: "ftp://localhost:4318", wantErr: true},
		{endpoint: "http://user:pass@localhost:4318", insecure: true, wantErr: true},
		{endpoint: "http://localhost:4318?query=1", insecure: true, wantErr: true},
	} {
		cfg.Protocol, cfg.Endpoint, cfg.TLS.Insecure = "http", tt.endpoint, tt.insecure
		assert.Equal(t, tt.wantErr, cfg.Validate() != nil, tt.endpoint)
	}
}
