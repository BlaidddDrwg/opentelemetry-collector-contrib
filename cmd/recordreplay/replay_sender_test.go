// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	replay "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/recordreplay"
)

type replayArrival struct {
	at      time.Time
	payload []byte
}

type replayLogsServer struct {
	plogotlp.UnimplementedGRPCServer
	receive func([]byte)
	partial bool
}

func (s *replayLogsServer) Export(_ context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	payload, err := req.MarshalProto()
	if err != nil {
		return plogotlp.ExportResponse{}, err
	}
	s.receive(payload)
	response := plogotlp.NewExportResponse()
	if s.partial {
		response.PartialSuccess().SetRejectedLogRecords(1)
	}
	return response, nil
}

type replayMetricsServer struct {
	pmetricotlp.UnimplementedGRPCServer
	receive func([]byte)
}

func (s *replayMetricsServer) Export(_ context.Context, req pmetricotlp.ExportRequest) (pmetricotlp.ExportResponse, error) {
	payload, err := req.MarshalProto()
	if err != nil {
		return pmetricotlp.ExportResponse{}, err
	}
	s.receive(payload)
	return pmetricotlp.NewExportResponse(), nil
}

type replayTracesServer struct {
	ptraceotlp.UnimplementedGRPCServer
	receive func([]byte)
}

func (s *replayTracesServer) Export(_ context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	payload, err := req.MarshalProto()
	if err != nil {
		return ptraceotlp.ExportResponse{}, err
	}
	s.receive(payload)
	return ptraceotlp.NewExportResponse(), nil
}

func startReplayGRPCServer(t *testing.T, receive func([]byte), partial bool, options ...grpc.ServerOption) string {
	t.Helper()
	var lc net.ListenConfig
	listener, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(options...)
	plogotlp.RegisterGRPCServer(server, &replayLogsServer{receive: receive, partial: partial})
	pmetricotlp.RegisterGRPCServer(server, &replayMetricsServer{receive: receive})
	ptraceotlp.RegisterGRPCServer(server, &replayTracesServer{receive: receive})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, <-done)
	})
	return listener.Addr().String()
}

func replayTestPayload(t *testing.T, signal string, index int) []byte {
	t.Helper()
	const eventTimestamp pcommon.Timestamp = 1234567890123456789
	var payload []byte
	var err error
	switch signal {
	case "logs":
		req := plogotlp.NewExportRequest()
		record := req.Logs().ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		record.Body().SetStr("log-" + strconv.Itoa(index))
		record.SetTimestamp(eventTimestamp)
		payload, err = req.MarshalProto()
	case "metrics":
		req := pmetricotlp.NewExportRequest()
		metric := req.Metrics().ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		metric.SetName("metric-" + strconv.Itoa(index))
		point := metric.SetEmptyGauge().DataPoints().AppendEmpty()
		point.SetTimestamp(eventTimestamp)
		point.SetIntValue(int64(index))
		payload, err = req.MarshalProto()
	case "traces":
		req := ptraceotlp.NewExportRequest()
		span := req.Traces().ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		span.SetName("span-" + strconv.Itoa(index))
		span.SetStartTimestamp(eventTimestamp)
		span.SetEndTimestamp(eventTimestamp + 100)
		payload, err = req.MarshalProto()
	}
	require.NoError(t, err)
	return payload
}

func TestReplayTransportsAndSignals(t *testing.T) {
	for _, protocol := range []string{"grpc", "http"} {
		for _, signal := range []string{"logs", "metrics", "traces"} {
			t.Run(protocol+"/"+signal, func(t *testing.T) {
				arrivals := make(chan replayArrival, 3)
				receive := func(data []byte) {
					arrivals <- replayArrival{at: time.Now(), payload: bytes.Clone(data)}
				}
				var endpoint string
				if protocol == "grpc" {
					endpoint = startReplayGRPCServer(t, receive, false)
				} else {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.Method != http.MethodPost || r.URL.Path != "/prefix/v1/"+signal || r.Header.Get("Content-Type") != "application/x-protobuf" {
							http.Error(w, "unexpected OTLP request", http.StatusBadRequest)
							return
						}
						data, err := io.ReadAll(r.Body)
						if err != nil {
							http.Error(w, err.Error(), http.StatusBadRequest)
							return
						}
						receive(data)
						w.Header().Set("Content-Type", "application/x-protobuf")
					}))
					t.Cleanup(server.Close)
					endpoint = server.URL + "/prefix"
				}
				records := make([]replay.Record, 3)
				offsets := []time.Duration{0, 150 * time.Millisecond, 350 * time.Millisecond}
				for i, offset := range offsets {
					records[i] = replay.Record{
						TimestampUnixNano: int64(time.Second + offset),
						Payload:           replayTestPayload(t, signal, i),
					}
				}
				original := recordingTestFile(t, records...)
				path := filepath.Join(t.TempDir(), "arbitrary-name.pb")
				require.NoError(t, os.WriteFile(path, original, 0o400))
				cmd := newCommand()
				cmd.SetArgs([]string{
					"replay", path, "--config", "config.yaml",
					"--set", "replay.endpoint=" + strconv.Quote(endpoint),
					"--set", "replay.protocol=" + protocol,
					"--set", "replay.signal=" + signal,
					"--set", "replay.tls.insecure=true",
					"--set", "replay.rebase_timestamps=false",
					"--set", "replay.lateness_tolerance=100ms",
				})
				var output, warnings bytes.Buffer
				cmd.SetOut(&output)
				cmd.SetErr(&warnings)
				require.NoError(t, cmd.ExecuteContext(t.Context()))
				assert.Contains(t, output.String(), "sent=3 backwards=0")
				assert.Empty(t, warnings.String())
				var firstArrival time.Time
				for i, record := range records {
					select {
					case arrival := <-arrivals:
						if i == 0 {
							firstArrival = arrival.at
						}
						assert.InDelta(t, offsets[i].Seconds(), arrival.at.Sub(firstArrival).Seconds(), 0.1,
							"real receiver arrivals should track first-record-relative offsets within integration-test tolerance")
						assert.Equal(t, record.Payload, arrival.payload, "telemetry contents and original event timestamps must survive replay")
					default:
						t.Fatal("missing replayed record")
					}
				}
				after, err := os.ReadFile(path)
				require.NoError(t, err)
				assert.Equal(t, original, after, "replay must not change its source")
			})
		}
	}
}

func TestReplayHTTPFailuresNotRetried(t *testing.T) {
	partial := plogotlp.NewExportResponse()
	partial.PartialSuccess().SetRejectedLogRecords(1)
	partialBytes, err := partial.MarshalProto()
	require.NoError(t, err)
	for _, tt := range []struct {
		name        string
		status      int
		contentType string
		body        []byte
		want        string
	}{
		{name: "server error", status: 503, want: "503"},
		{name: "redirect", status: 307, want: "307"},
		{name: "wrong content type", status: 200, contentType: "text/plain", want: "Content-Type"},
		{name: "invalid response", status: 200, contentType: "application/x-protobuf", body: []byte{255}, want: "export failed"},
		{name: "partial rejection", status: 200, contentType: "application/x-protobuf", body: partialBytes, want: "partial-success"},
		{name: "oversized response", status: 200, contentType: "application/x-protobuf", body: make([]byte, 1024*1024+1), want: "exceeds 1 MiB"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", tt.contentType)
				w.Header().Set("Location", "/redirect-target")
				w.WriteHeader(tt.status)
				_, _ = w.Write(tt.body)
			}))
			t.Cleanup(server.Close)
			cfg := replay.DefaultConfig()
			cfg.File = filepath.Join(t.TempDir(), "logs.pb")
			cfg.Protocol, cfg.Endpoint, cfg.Signal, cfg.TLS.Insecure = "http", server.URL, "logs", true
			data := recordingTestFile(t, replay.Record{Payload: replayTestPayload(t, "logs", 0)})
			require.NoError(t, os.WriteFile(cfg.File, data, 0o400))
			err := replayFile(t.Context(), cfg, io.Discard, io.Discard, defaultProgressInterval)
			require.ErrorContains(t, err, tt.want)
			assert.Equal(t, int32(1), calls.Load())
		})
	}
}

func TestReplayGRPCPartialSuccess(t *testing.T) {
	var calls atomic.Int32
	endpoint := startReplayGRPCServer(t, func([]byte) { calls.Add(1) }, true)
	cfg := replay.DefaultConfig()
	cfg.Endpoint, cfg.Signal, cfg.TLS.Insecure = endpoint, "logs", true
	sender, err := replay.NewSender(t.Context(), cfg, false)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, sender.Close()) })
	send, err := sender.Prepare(replayTestPayload(t, "logs", 0), 0)
	require.NoError(t, err)
	require.ErrorContains(t, send(t.Context()), "rejected=1")
	assert.Equal(t, int32(1), calls.Load())
}

func TestReplayGRPCConnectionCancellation(t *testing.T) {
	cfg := replay.DefaultConfig()
	cfg.Endpoint, cfg.Signal, cfg.TLS.Insecure = "127.0.0.1:1", "logs", true
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := replay.NewSender(ctx, cfg, false)
	require.ErrorIs(t, err, context.Canceled)
}

func TestReplayInvalidPayloadNeverSent(t *testing.T) {
	for _, protocol := range []string{"grpc", "http"} {
		cfg := replay.DefaultConfig()
		cfg.Protocol, cfg.Signal, cfg.TLS.Insecure = protocol, "logs", true
		if protocol == "grpc" {
			cfg.Endpoint = startReplayGRPCServer(t, func([]byte) { t.Error("invalid payload was sent") }, false)
		} else {
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Error("invalid payload was sent")
			}))
			t.Cleanup(server.Close)
			cfg.Endpoint = server.URL
		}
		for _, signal := range []string{"logs", "metrics", "traces"} {
			cfg.Signal = signal
			sender, err := replay.NewSender(t.Context(), cfg, false)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, sender.Close()) })
			_, err = sender.Prepare([]byte{255}, 0)
			require.Error(t, err)
		}
	}
}

func TestReplayHTTPS(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	t.Cleanup(server.Close)
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{
		Type: "CERTIFICATE", Bytes: server.Certificate().Raw,
	}), 0o600))
	cfg := replay.DefaultConfig()
	cfg.Protocol, cfg.Endpoint, cfg.Signal = "http", server.URL, "logs"
	cfg.TLS.CAFile = caPath
	sender, err := replay.NewSender(t.Context(), cfg, false)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, sender.Close()) })
	send, err := sender.Prepare(replayTestPayload(t, "logs", 0), 0)
	require.NoError(t, err)
	require.NoError(t, send(t.Context()))
}

func TestReplayGRPCTLS(t *testing.T) {
	certServer := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(certServer.Close)
	var calls atomic.Int32
	endpoint := startReplayGRPCServer(t, func([]byte) { calls.Add(1) }, false,
		grpc.Creds(credentials.NewTLS(&tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: certServer.TLS.Certificates,
		})),
	)
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{
		Type: "CERTIFICATE", Bytes: certServer.Certificate().Raw,
	}), 0o600))
	cfg := replay.DefaultConfig()
	cfg.Endpoint, cfg.Signal, cfg.TLS.CAFile = endpoint, "logs", caPath
	sender, err := replay.NewSender(t.Context(), cfg, false)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, sender.Close()) })
	send, err := sender.Prepare(replayTestPayload(t, "logs", 0), 0)
	require.NoError(t, err)
	require.NoError(t, send(t.Context()))
	assert.Equal(t, int32(1), calls.Load())
}

func TestReplayIncompleteTailSendsPrefixWithoutRepair(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	t.Cleanup(server.Close)
	cfg := replay.DefaultConfig()
	cfg.Endpoint, cfg.Signal, cfg.Protocol, cfg.TLS.Insecure = server.URL, "logs", "http", true
	cfg.File = filepath.Join(t.TempDir(), "input.pb")
	data := append(recordingTestFile(t, replay.Record{Payload: replayTestPayload(t, "logs", 0)}), 0, 0)
	require.NoError(t, os.WriteFile(cfg.File, data, 0o400))
	var output bytes.Buffer
	err := replayFile(t.Context(), cfg, io.Discard, &output, defaultProgressInterval)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	assert.Contains(t, output.String(), "sent=1")
	assert.Equal(t, int32(1), calls.Load())
	after, err := os.ReadFile(cfg.File)
	require.NoError(t, err)
	assert.Equal(t, data, after)
}
