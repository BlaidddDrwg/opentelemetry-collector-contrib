// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	replay "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/recordreplay"
)

func replayPayloadTimestamp(t *testing.T, signal string, payload []byte) pcommon.Timestamp {
	t.Helper()
	switch signal {
	case "logs":
		req := plogotlp.NewExportRequest()
		require.NoError(t, req.UnmarshalProto(payload))
		return req.Logs().ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Timestamp()
	case "metrics":
		req := pmetricotlp.NewExportRequest()
		require.NoError(t, req.UnmarshalProto(payload))
		return req.Metrics().ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0).Timestamp()
	default:
		req := ptraceotlp.NewExportRequest()
		require.NoError(t, req.UnmarshalProto(payload))
		return req.Traces().ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).StartTimestamp()
	}
}

func TestReplayRebasedTransports(t *testing.T) {
	for _, protocol := range []string{"grpc", "http"} {
		for _, signal := range []string{"logs", "metrics", "traces"} {
			t.Run(protocol+"/"+signal, func(t *testing.T) {
				arrivals := make(chan []byte, 2)
				cfg := replay.DefaultConfig()
				cfg.Protocol, cfg.Signal, cfg.TLS.Insecure = protocol, signal, true
				cfg.RebaseTimestamps = true
				cfg.LatenessTolerance = time.Second
				if protocol == "grpc" {
					cfg.Endpoint = startReplayGRPCServer(t, func(payload []byte) { arrivals <- payload }, false)
				} else {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						var payload bytes.Buffer
						_, _ = payload.ReadFrom(r.Body)
						arrivals <- payload.Bytes()
						w.Header().Set("Content-Type", "application/x-protobuf")
					}))
					t.Cleanup(server.Close)
					cfg.Endpoint = server.URL
				}
				first := replayTestPayload(t, signal, 0)
				originalTime := replayPayloadTimestamp(t, signal, first)
				data := recordingTestFile(t,
					replay.Record{TimestampUnixNano: int64(originalTime), Payload: first},
					replay.Record{TimestampUnixNano: int64(originalTime) + int64(100*time.Millisecond), Payload: replayTestPayload(t, signal, 1)},
				)
				cfg.File = filepath.Join(t.TempDir(), "input.pb")
				require.NoError(t, os.WriteFile(cfg.File, data, 0o400))
				var output bytes.Buffer
				before := time.Now()
				require.NoError(t, replayFile(t.Context(), cfg, &output, &output, 0))
				after := time.Now()
				assert.Contains(t, output.String(), "rebase_timestamps=true")
				assert.Contains(t, output.String(), "sent=2")
				firstRebased := replayPayloadTimestamp(t, signal, <-arrivals)
				secondRebased := replayPayloadTimestamp(t, signal, <-arrivals)
				assert.GreaterOrEqual(t, int64(firstRebased), before.UnixNano())
				assert.LessOrEqual(t, int64(firstRebased), after.UnixNano())
				assert.Equal(t, firstRebased, secondRebased, "identical original event times get the same session shift, not a new per-request timestamp")
				assert.NotEqual(t, originalTime, firstRebased)
				unchanged, err := os.ReadFile(cfg.File)
				require.NoError(t, err)
				assert.Equal(t, data, unchanged)
			})
		}
	}
}
