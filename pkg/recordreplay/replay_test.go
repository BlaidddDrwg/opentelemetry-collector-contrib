// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

func testPayload(t *testing.T, signal string) []byte {
	t.Helper()
	var data []byte
	var err error
	switch signal {
	case "logs":
		request := plogotlp.NewExportRequest()
		request.Logs().ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().SetTimestamp(100)
		data, err = request.MarshalProto()
	case "metrics":
		request := pmetricotlp.NewExportRequest()
		request.Metrics().ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetEmptyGauge().DataPoints().AppendEmpty().SetTimestamp(100)
		data, err = request.MarshalProto()
	case "traces":
		request := ptraceotlp.NewExportRequest()
		request.Traces().ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetStartTimestamp(100)
		data, err = request.MarshalProto()
	}
	require.NoError(t, err)
	return data
}

func testFile(t *testing.T, records []Record, header bool) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "recording.pb")
	var data bytes.Buffer
	if header {
		require.NoError(t, WriteHeader(&data))
	}
	for _, record := range records {
		require.NoError(t, WriteFrame(&data, record))
	}
	require.NoError(t, os.WriteFile(path, data.Bytes(), 0o600))
	return path
}

func TestInspectCountsAndTimestampExtrema(t *testing.T) {
	for _, signal := range []string{"logs", "metrics", "traces"} {
		t.Run(signal, func(t *testing.T) {
			payload := bytes.Repeat(testPayload(t, signal), 3)
			path := testFile(t, []Record{
				{TimestampUnixNano: 300, Payload: payload},
				{TimestampUnixNano: 100, Payload: payload},
				{TimestampUnixNano: 200, Payload: payload},
			}, true)
			original, err := os.ReadFile(path)
			require.NoError(t, err)
			file, size, err := OpenInput(path)
			require.NoError(t, err)
			defer func() { require.NoError(t, file.Close()) }()
			got, err := Inspect(t.Context(), file, size, signal, 1024)
			require.NoError(t, err)
			assert.Equal(t, uint64(3), got.Batches)
			assert.Equal(t, uint64(9), got.Items)
			assert.Equal(t, int64(100), got.FirstTimestampUnixNano)
			assert.Equal(t, int64(300), got.LastTimestampUnixNano)
			assert.Equal(t, int64(200), got.DurationNanos)
			assert.Equal(t, int64(len(original)), got.SizeBytes)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			assert.Equal(t, original, after)
		})
	}
}

func TestInspectEmptyAndCorrupt(t *testing.T) {
	for _, header := range []bool{false, true} {
		file, size, err := OpenInput(testFile(t, nil, header))
		require.NoError(t, err)
		got, err := Inspect(t.Context(), file, size, "logs", 1024)
		require.NoError(t, err)
		require.NoError(t, file.Close())
		assert.Zero(t, got.Batches)
		assert.Zero(t, got.Items)
		assert.Zero(t, got.DurationNanos)
	}
	for _, kind := range []string{"payload", "tail", "oversized", "header", "duration-overflow"} {
		t.Run(kind, func(t *testing.T) {
			records := []Record{{TimestampUnixNano: 100, Payload: testPayload(t, "logs")}}
			if kind == "payload" {
				records[0].Payload = []byte{255}
			}
			if kind == "duration-overflow" {
				records[0].TimestampUnixNano = math.MinInt64
				records = append(records, Record{TimestampUnixNano: math.MaxInt64})
			}
			path := testFile(t, records, kind != "header")
			if kind == "tail" {
				file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
				require.NoError(t, err)
				_, err = file.Write([]byte{0, 0})
				require.NoError(t, err)
				require.NoError(t, file.Close())
			}
			file, size, err := OpenInput(path)
			require.NoError(t, err)
			defer func() { require.NoError(t, file.Close()) }()
			limit := uint32(1024)
			if kind == "oversized" {
				limit = 1
			}
			_, err = Inspect(t.Context(), file, size, "logs", limit)
			require.Error(t, err)
		})
	}
	_, _, err := OpenInput(t.TempDir())
	require.Error(t, err)
}

func TestInspectionBoundedSnapshotAndCancellation(t *testing.T) {
	path := testFile(t, []Record{{Payload: testPayload(t, "logs")}}, true)
	file, size, err := OpenInput(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()
	appendFile, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	require.NoError(t, WriteFrame(appendFile, Record{Payload: []byte{255}}))
	require.NoError(t, appendFile.Close())
	got, err := Inspect(t.Context(), file, size, "logs", 1024)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), got.Batches, "new appends must not enter the fixed snapshot")
	_, err = NewReader(file, -1)
	require.Error(t, err)
	_, err = NewReader(file, math.MaxInt64)
	require.Error(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = Inspect(ctx, file, size, "logs", 1024)
	require.ErrorIs(t, err, context.Canceled)
	link := filepath.Join(t.TempDir(), "link.pb")
	require.NoError(t, os.Symlink(path, link))
	_, _, err = OpenInput(link)
	require.Error(t, err)
}

func TestScheduledCommonOriginAndRebase(t *testing.T) {
	for _, rebase := range []bool{false, true} {
		for _, offset := range []time.Duration{0, 2 * time.Second} {
			t.Run(offset.String()+"/rebase="+map[bool]string{false: "false", true: "true"}[rebase], func(t *testing.T) {
				origin := int64(1000)
				epoch := time.Unix(100, 0)
				now := epoch.Add(-time.Second)
				schedule := &Schedule{OriginUnixNano: origin, Start: epoch}
				if rebase {
					schedule.TimestampShift = time.Duration(epoch.UnixNano() - origin)
				}
				cfg := DefaultConfig()
				cfg.Signal = "logs"
				record := Record{TimestampUnixNano: origin + int64(offset), Payload: testPayload(t, "logs")}
				clock := Clock{Now: func() time.Time { return now }, Wait: func(_ context.Context, deadline time.Time) error {
					assert.Equal(t, epoch.Add(offset), deadline)
					now = deadline
					return nil
				}}
				var states []string
				stats, err := Replay(t.Context(), bytes.NewReader(nil), record, cfg,
					func(_ []byte, shift time.Duration) (func(context.Context) error, error) {
						assert.Equal(t, schedule.TimestampShift, shift, "shift must not depend on this file's first timestamp")
						return func(context.Context) error { return nil }, nil
					}, clock, schedule, io.Discard, func(p Progress) { states = append(states, p.State) })
				require.NoError(t, err)
				assert.Equal(t, uint64(1), stats.Attempted)
				assert.Equal(t, uint64(1), stats.Accepted)
				assert.Equal(t, []string{"preparing", "waiting", "sending", "sending", "reading"}, states)
			})
		}
	}
}

func TestItemAccounting(t *testing.T) {
	for _, tt := range []struct {
		name               string
		sendErr            error
		accepted, failed   uint64
		ambiguous, batches uint64
	}{
		{name: "accepted", accepted: 3, batches: 1},
		{name: "network failure", sendErr: errors.New("endpoint failure"), ambiguous: 3},
		{name: "partial", sendErr: &PartialError{Rejected: 1, Message: "diagnostic"}, accepted: 2, failed: 1, batches: 1},
		{name: "all rejected", sendErr: &PartialError{Rejected: 3}, failed: 3, batches: 1},
		{name: "invalid rejection", sendErr: &PartialError{Rejected: 4}, ambiguous: 3},
		{name: "negative rejection", sendErr: &PartialError{Rejected: -1}, ambiguous: 3},
		{name: "unquantified partial warning", sendErr: &PartialError{Message: "diagnostic"}, ambiguous: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Signal = "logs"
			now := time.Now()
			record := Record{TimestampUnixNano: 100, Payload: bytes.Repeat(testPayload(t, "logs"), 3)}
			var progress Progress
			stats, err := Replay(t.Context(), bytes.NewReader(nil), record, cfg,
				func([]byte, time.Duration) (func(context.Context) error, error) {
					return func(context.Context) error {
						assert.Equal(t, "sending", progress.State)
						assert.Equal(t, uint64(3), progress.Stats.Attempted)
						assert.Zero(t, progress.Stats.Accepted)
						return tt.sendErr
					}, nil
				}, Clock{Now: func() time.Time { return now }, Wait: func(context.Context, time.Time) error { return nil }},
				&Schedule{Start: now, OriginUnixNano: 100}, io.Discard, func(p Progress) { progress = p })
			if tt.sendErr != nil {
				require.ErrorIs(t, err, tt.sendErr, "generic errors retain useful CLI diagnostics")
				assert.Zero(t, stats.Sent)
			} else {
				require.NoError(t, err)
				assert.Equal(t, uint64(1), stats.Sent)
			}
			assert.Equal(t, tt.accepted, stats.Accepted)
			assert.Equal(t, tt.failed, stats.Failed)
			assert.Equal(t, tt.ambiguous, stats.Ambiguous)
			assert.Equal(t, tt.batches, stats.Batches)
			assert.Equal(t, tt.batches*uint64(len(record.Payload)), stats.PayloadBytes)
			assert.Equal(t, stats, progress.Stats)
		})
	}
}

func TestScheduledOffsetBoundsAndStrictBackwards(t *testing.T) {
	for _, tt := range []struct {
		name       string
		origin     int64
		timestamps []int64
		late       bool
		wantSent   uint64
	}{
		{name: "before origin", origin: 100, timestamps: []int64{99}},
		{name: "overflow", origin: math.MinInt64, timestamps: []int64{math.MaxInt64}},
		{name: "strict backwards", timestamps: []int64{100, 200, 150}, late: true, wantSent: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Signal = "logs"
			cfg.LatenessTolerance = 0
			now := time.Unix(100, 0)
			schedule := &Schedule{Start: now, OriginUnixNano: tt.origin}
			var data bytes.Buffer
			payload := testPayload(t, "logs")
			for _, timestamp := range tt.timestamps[1:] {
				require.NoError(t, WriteFrame(&data, Record{TimestampUnixNano: timestamp, Payload: payload}))
			}
			var warnings bytes.Buffer
			stats, err := Replay(t.Context(), &data, Record{TimestampUnixNano: tt.timestamps[0], Payload: payload}, cfg,
				func([]byte, time.Duration) (func(context.Context) error, error) {
					return func(context.Context) error { return nil }, nil
				},
				Clock{Now: func() time.Time { return now }, Wait: func(_ context.Context, deadline time.Time) error { now = maxTime(now, deadline); return nil }},
				schedule, &warnings, nil)
			require.Error(t, err)
			assert.Equal(t, tt.wantSent, stats.Sent)
			assert.Empty(t, warnings.String())
			if tt.late {
				var late *LatenessError
				require.ErrorAs(t, err, &late)
				assert.Equal(t, uint64(3), late.Record)
				assert.Equal(t, 50*time.Nanosecond, late.Late)
				assert.Zero(t, late.Tolerance)
				assert.Equal(t, uint64(2), stats.Attempted)
			}
		})
	}
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
