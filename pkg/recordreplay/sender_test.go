// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"bytes"
	"context"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
)

func TestHTTPConnectionFailurePreservesCause(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Signal, cfg.Protocol, cfg.Endpoint, cfg.TLS.Insecure = "logs", "http", "http://127.0.0.1:0", true
	_, err := NewSender(t.Context(), cfg, true)
	var dialErr *net.OpError
	require.ErrorAs(t, err, &dialErr)
	assert.Contains(t, err.Error(), "cannot establish replay HTTP connection")
}

func TestHTTPPreparedConnection(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(map[bool]string{false: "http", true: "https"}[secure], func(t *testing.T) {
			var requests atomic.Int64
			payloads := make(chan []byte, 1)
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				data, err := io.ReadAll(r.Body)
				assert.NoError(t, err)
				payloads <- data
				w.Header().Set("Content-Type", "application/x-protobuf")
			})
			server := httptest.NewUnstartedServer(handler)
			if secure {
				server.StartTLS()
			} else {
				server.Start()
			}
			defer server.Close()
			cfg := DefaultConfig()
			cfg.Signal, cfg.Protocol, cfg.Endpoint = "logs", "http", server.URL
			cfg.TLS.Insecure = !secure
			if secure {
				caPath := filepath.Join(t.TempDir(), "ca.pem")
				require.NoError(t, os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600))
				cfg.TLS.CAFile = caPath
			}
			sender, err := NewSender(t.Context(), cfg, true)
			require.NoError(t, err)
			defer func() { require.NoError(t, sender.Close()) }()
			assert.Zero(t, requests.Load(), "connection readiness must not export telemetry")
			send, err := sender.Prepare(testPayload(t, "logs"), 10)
			require.NoError(t, err)
			require.NoError(t, send(t.Context()))
			assert.Equal(t, int64(1), requests.Load())
			request := plogotlp.NewExportRequest()
			require.NoError(t, request.UnmarshalProto(<-payloads))
			assert.Equal(t, uint64(110), uint64(request.Logs().ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Timestamp()))
		})
	}
}

func TestHTTPInFlightCancellationAccounting(t *testing.T) {
	received := make(chan struct{})
	requestCancelled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(received)
		<-r.Context().Done()
		close(requestCancelled)
	}))
	defer server.Close()
	cfg := DefaultConfig()
	cfg.Signal, cfg.Protocol, cfg.Endpoint, cfg.TLS.Insecure = "logs", "http", server.URL, true
	cfg.LatenessTolerance = time.Second
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sender, err := NewSender(ctx, cfg, true)
	require.NoError(t, err)
	defer func() { require.NoError(t, sender.Close()) }()
	var inFlight, final Progress
	record := Record{TimestampUnixNano: 100, Payload: bytes.Repeat(testPayload(t, "logs"), 3)}
	done := make(chan error, 1)
	go func() {
		_, replayErr := Replay(ctx, bytes.NewReader(nil), record, cfg,
			sender.Prepare, Clock{}, &Schedule{Start: time.Now(), OriginUnixNano: 100}, io.Discard,
			func(p Progress) {
				final = p
				if p.State == "sending" && p.Stats.Attempted > 0 && p.Stats.Ambiguous == 0 {
					inFlight = p
				}
			})
		done <- replayErr
	}()
	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("replay never sent HTTP request")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	assert.Equal(t, uint64(3), inFlight.Stats.Attempted)
	assert.Zero(t, inFlight.Stats.Accepted)
	assert.Equal(t, uint64(3), final.Stats.Ambiguous)
	assert.Zero(t, final.Stats.Accepted)
	select {
	case <-requestCancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not reach the HTTP request")
	}
}
