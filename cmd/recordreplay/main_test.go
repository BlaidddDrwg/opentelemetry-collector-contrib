// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
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

func TestCommands(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantErr    string
		wantOutput string
	}{
		{
			name:       "help",
			args:       []string{"--help"},
			wantOutput: "record",
		},
		{
			name:    "record requires config",
			args:    []string{"record"},
			wantErr: "at least one config flag must be provided",
		},
		{
			name: "validate example",
			args: []string{"record", "validate", "--config", "config.yaml"},
		},
		{
			name: "validate capture duration override",
			args: []string{"record", "validate", "--config", "config.yaml", "--set", "record.duration=30s"},
		},
		{
			name:    "validate rejects unlimited capture",
			args:    []string{"record", "validate", "--config", "config.yaml", "--set", "record.duration=0s"},
			wantErr: "record.duration must be positive",
		},
		{
			name:    "validate rejects negative capture",
			args:    []string{"record", "validate", "--config", "config.yaml", "--set", "record.duration=-1s"},
			wantErr: "record.duration must be positive",
		},
		{
			name:    "append and overwrite are mutually exclusive",
			args:    []string{"record", "--config", "config.yaml", "--append", "--overwrite"},
			wantErr: "none of the others can be",
		},
		{
			name: "validate append without prompting",
			args: []string{"record", "validate", "--config", "config.yaml", "--append"},
		},
		{
			name: "validate append rejects rotation",
			args: []string{
				"record", "validate", "--config", "config.yaml", "--append",
				"--set", "exporters.file/logs.rotation.max_megabytes=100",
				"--set", "exporters.file/logs.sync_interval=0s",
			},
			wantErr: "append and rotation",
		},
		{
			name:    "validate rejects JSON",
			args:    []string{"record", "validate", "--config", "config.yaml", "--set", "exporters.file/logs.format=json"},
			wantErr: "versioned recordings require format: proto",
		},
		{
			name:    "validate rejects compression",
			args:    []string{"record", "validate", "--config", "config.yaml", "--set", "exporters.file/logs.compression=zstd"},
			wantErr: "without compression, an encoding extension, or rotation",
		},
		{
			name: "validate rejects rotation without append",
			args: []string{
				"record", "validate", "--config", "config.yaml",
				"--set", "exporters.file/logs.rotation.max_megabytes=100",
				"--set", "exporters.file/logs.sync_interval=0s",
			},
			wantErr: "without compression, an encoding extension, or rotation",
		},
		{
			name:       "replay help",
			args:       []string{"replay", "--help"},
			wantOutput: "offsets from its first timestamp",
		},
		{
			name:    "replay with file",
			args:    []string{"replay", "does-not-exist.pb", "--config", "config.yaml"},
			wantErr: "no such file",
		},
		{
			name:    "record rejects negative progress interval",
			args:    []string{"record", "--config", "config.yaml", "--progress-interval=-1s"},
			wantErr: "progress-interval must not be negative",
		},
		{
			name:    "replay rejects negative progress interval",
			args:    []string{"replay", "--config", "config.yaml", "--progress-interval=-1s"},
			wantErr: "progress-interval must not be negative",
		},
		{
			name:    "unknown command",
			args:    []string{"unknown"},
			wantErr: "unknown command",
		},
		{
			name:    "record rejects positional arguments",
			args:    []string{"record", "unexpected"},
			wantErr: "unknown command",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newCommand()
			var output bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetErr(&output)
			cmd.SetIn(strings.NewReader(""))
			cmd.SetArgs(tt.args)
			err := cmd.ExecuteContext(t.Context())
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Contains(t, output.String(), tt.wantOutput)
		})
	}
}

func TestRecord(t *testing.T) {
	traces := ptraceotlp.NewExportRequest()
	traces.Traces().ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("recorded-span")
	traceBytes, err := traces.MarshalProto()
	require.NoError(t, err)

	metrics := pmetricotlp.NewExportRequest()
	metric := metrics.Metrics().ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("recorded-metric")
	metric.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(42)
	metricBytes, err := metrics.MarshalProto()
	require.NoError(t, err)

	logs := plogotlp.NewExportRequest()
	logs.Logs().ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("recorded-log")
	logBytes, err := logs.MarshalProto()
	require.NoError(t, err)

	payloads := []struct {
		signal string
		data   []byte
	}{
		{"traces", traceBytes},
		{"metrics", metricBytes},
		{"logs", logBytes},
	}

	for _, tt := range []struct {
		protocol string
		mode     string
		tail     []byte
	}{
		{protocol: "http", mode: "overwrite"},
		{protocol: "grpc"},
		{protocol: "grpc", mode: "append"},
		{protocol: "grpc", mode: "append", tail: []byte{0, 0, 0, 10, 1, 2}},
	} {
		name := tt.protocol + "/" + tt.mode
		if len(tt.tail) > 0 {
			name += "/incomplete-tail"
		}
		t.Run(name, func(t *testing.T) {
			protocol := tt.protocol
			var listenConfig net.ListenConfig
			listener, listenErr := listenConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, listenErr)
			endpoint := listener.Addr().String()
			require.NoError(t, listener.Close())

			otherProtocol := "http"
			if protocol == "http" {
				otherProtocol = "grpc"
			}
			dir := t.TempDir()
			args := []string{
				"record", "--config", "config.yaml",
				"--progress-interval=20ms",
				"--set", "receivers.otlp.protocols." + protocol + ".endpoint=" + endpoint,
				"--set", "receivers.otlp.protocols." + otherProtocol + ".endpoint=127.0.0.1:0",
			}
			for _, payload := range payloads {
				args = append(args, "--set", "exporters.file/"+payload.signal+".path="+strconv.Quote(filepath.Join(dir, payload.signal+".pb")))
			}
			var output bytes.Buffer
			var previousRecording []byte
			switch tt.mode {
			case "overwrite":
				require.NoError(t, os.WriteFile(filepath.Join(dir, "logs.pb"), []byte("old recording"), 0o600))
				args = append(args, "--overwrite")
				for _, payload := range payloads {
					args = append(args, "--set", "exporters.file/"+payload.signal+".append=true")
				}
			case "append":
				previousRecording = recordingTestFile(t, replay.Record{TimestampUnixNano: 1, Payload: logBytes})
				require.NoError(t, os.WriteFile(filepath.Join(dir, "logs.pb"), append(bytes.Clone(previousRecording), tt.tail...), 0o600))
				args = append(args, "--append", "--set", "exporters.file/logs.append=false")
			}

			recordingStarted := time.Now().UnixNano()
			ctx, cancel := context.WithCancel(t.Context())
			ready := make(chan struct{})
			var readyOnce sync.Once
			cmd := newCommand(zap.Hooks(func(entry zapcore.Entry) error {
				if entry.Message == "Everything is ready. Begin running and processing data." {
					readyOnce.Do(func() { close(ready) })
				}
				return nil
			}))
			cmd.SetArgs(args)
			cmd.SetOut(&output)
			cmd.SetErr(&output)
			cmd.SetIn(strings.NewReader("yes\n"))
			done := make(chan error, 1)
			go func() {
				done <- cmd.ExecuteContext(ctx)
				close(done)
			}()
			stop := func() {
				cancel()
				select {
				case runErr := <-done:
					assert.NoError(t, runErr)
				case <-time.After(10 * time.Second):
					t.Error("recorder did not shut down")
				}
			}
			t.Cleanup(stop)

			select {
			case <-ready:
			case runErr := <-done:
				t.Fatalf("recorder exited before becoming ready: %v", runErr)
			case <-time.After(10 * time.Second):
				t.Fatal("recorder did not become ready")
			}

			for session := range 2 {
				t.Run("connection"+strconv.Itoa(session+1), func(t *testing.T) {
					requestCtx, requestCancel := context.WithTimeout(ctx, 5*time.Second)
					defer requestCancel()
					if protocol == "http" {
						client := &http.Client{Transport: &http.Transport{}, Timeout: 5 * time.Second}
						t.Cleanup(client.CloseIdleConnections)
						for _, payload := range payloads {
							req, requestErr := http.NewRequestWithContext(requestCtx, http.MethodPost, "http://"+endpoint+"/v1/"+payload.signal, bytes.NewReader(payload.data))
							require.NoError(t, requestErr)
							req.Header.Set("Content-Type", "application/x-protobuf")
							resp, sendErr := client.Do(req)
							require.NoError(t, sendErr)
							_, copyErr := io.Copy(io.Discard, resp.Body)
							require.NoError(t, resp.Body.Close())
							require.NoError(t, copyErr)
							require.Equal(t, http.StatusOK, resp.StatusCode)
						}
					} else {
						conn, clientErr := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
						require.NoError(t, clientErr)
						t.Cleanup(func() { assert.NoError(t, conn.Close()) })
						_, exportErr := ptraceotlp.NewGRPCClient(conn).Export(requestCtx, traces)
						require.NoError(t, exportErr)
						_, exportErr = pmetricotlp.NewGRPCClient(conn).Export(requestCtx, metrics)
						require.NoError(t, exportErr)
						_, exportErr = plogotlp.NewGRPCClient(conn).Export(requestCtx, logs)
						require.NoError(t, exportErr)
					}
				})
				for _, payload := range payloads {
					expectedRecords := session + 1
					if tt.mode == "append" && payload.signal == "logs" {
						expectedRecords++
					}
					require.Eventually(t, func() bool {
						data, readErr := os.ReadFile(filepath.Join(dir, payload.signal+".pb"))
						if readErr != nil {
							return false
						}
						reader := bytes.NewReader(data)
						if _, readErr = replay.ReadHeader(reader); readErr != nil {
							return false
						}
						for range expectedRecords {
							record, frameErr := replay.ReadFrame(reader, uint32(len(payload.data)))
							if frameErr != nil || !bytes.Equal(record.Payload, payload.data) {
								return false
							}
						}
						_, readErr = replay.ReadFrame(reader, uint32(len(payload.data)))
						return errors.Is(readErr, io.EOF)
					}, 5*time.Second, 10*time.Millisecond, "%s from connection %d must be flushed while the recorder is still running", payload.signal, session+1)
				}
			}
			stop()
			recordingStopped := time.Now().UnixNano()
			if tt.mode != "" {
				assert.Contains(t, output.String(), "WARNING: --"+tt.mode)
				assert.Contains(t, output.String(), "Type yes")
			} else {
				assert.NotContains(t, output.String(), "Type yes", "fresh output files must not prompt")
			}
			assert.Contains(t, output.String(), "Recording started:")
			assert.Contains(t, output.String(), "Recording progress")
			assert.Contains(t, output.String(), "Recording: status=complete")
			assert.Contains(t, output.String(), "batches=6 ", "count only batches written during this session, not existing appended records")
			assert.Equal(t, len(tt.tail) > 0, strings.Contains(output.String(), "removing incomplete trailing frame"))

			for _, payload := range payloads {
				data, readErr := os.ReadFile(filepath.Join(dir, payload.signal+".pb"))
				require.NoError(t, readErr)
				frameSize := 12 + len(payload.data)
				expectedRecords := 2
				if tt.mode == "append" && payload.signal == "logs" {
					expectedRecords++
				}
				reader := bytes.NewReader(data)
				header, headerErr := replay.ReadHeader(reader)
				require.NoError(t, headerErr)
				assert.Equal(t, uint16(1), header.Version)
				headerSize := len(data) - reader.Len()
				require.Len(t, data, headerSize+expectedRecords*frameSize, "exactly one file header must be present")
				for i := range expectedRecords {
					offset := headerSize + i*frameSize
					assert.Equal(t, uint32(8+len(payload.data)), binary.BigEndian.Uint32(data[offset:offset+4]))
					assert.Equal(t, payload.data, data[offset+12:offset+frameSize])
					recorded, frameErr := replay.ReadFrame(reader, uint32(len(payload.data)))
					require.NoError(t, frameErr)
					assert.Equal(t, payload.data, recorded.Payload)
					assert.Equal(t, int64(binary.BigEndian.Uint64(data[offset+4:offset+12])), recorded.TimestampUnixNano)
					if tt.mode == "append" && payload.signal == "logs" && i == 0 {
						assert.Equal(t, int64(1), recorded.TimestampUnixNano, "appending must preserve existing timestamps")
						assert.True(t, bytes.HasPrefix(data, previousRecording))
					} else {
						assert.GreaterOrEqual(t, recorded.TimestampUnixNano, recordingStarted)
						assert.LessOrEqual(t, recorded.TimestampUnixNano, recordingStopped)
					}
				}
				_, frameErr := replay.ReadFrame(reader, uint32(len(payload.data)))
				require.ErrorIs(t, frameErr, io.EOF)
			}
		})
	}
}

func TestRecordRefusesExistingFiles(t *testing.T) {
	for _, existing := range [][]string{{"traces"}, {"metrics"}, {"logs"}, {"traces", "metrics", "logs"}} {
		t.Run(existing[0]+strconv.Itoa(len(existing)), func(t *testing.T) {
			dir := t.TempDir()
			args := []string{"record", "--config", "config.yaml"}
			for _, signal := range []string{"traces", "metrics", "logs"} {
				args = append(args, "--set", "exporters.file/"+signal+".path="+strconv.Quote(filepath.Join(dir, signal+".pb")))
			}
			for _, signal := range existing {
				require.NoError(t, os.WriteFile(filepath.Join(dir, signal+".pb"), []byte("keep me"), 0o600))
			}
			cmd := newCommand()
			cmd.SetArgs(args)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			require.ErrorContains(t, cmd.ExecuteContext(t.Context()), "refusing to start")
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.Len(t, entries, len(existing), "no exporter may open before all paths are checked")
			for _, signal := range existing {
				data, readErr := os.ReadFile(filepath.Join(dir, signal+".pb"))
				require.NoError(t, readErr)
				assert.Equal(t, "keep me", string(data))
			}
		})
	}
}

func TestRecordRequiresConfirmation(t *testing.T) {
	for _, mode := range []string{"append", "overwrite"} {
		for _, answer := range []string{"no\n", ""} {
			t.Run(mode+"/"+strconv.Itoa(len(answer)), func(t *testing.T) {
				dir := t.TempDir()
				existing := filepath.Join(dir, "logs.pb")
				require.NoError(t, os.WriteFile(existing, []byte("keep me"), 0o600))
				args := []string{"record", "--config", "config.yaml", "--" + mode}
				for _, signal := range []string{"traces", "metrics", "logs"} {
					args = append(args, "--set", "exporters.file/"+signal+".path="+strconv.Quote(filepath.Join(dir, signal+".pb")))
				}
				cmd := newCommand()
				cmd.SetArgs(args)
				cmd.SetIn(strings.NewReader(answer))
				var output bytes.Buffer
				cmd.SetOut(&output)
				cmd.SetErr(&output)
				require.ErrorContains(t, cmd.ExecuteContext(t.Context()), "recording cancelled")
				assert.Contains(t, output.String(), "WARNING: --"+mode)
				assert.NotContains(t, output.String(), "Recording started:", "do not start reporting before confirmation")
				assert.Contains(t, output.String(), existing)
				entries, err := os.ReadDir(dir)
				require.NoError(t, err)
				require.Len(t, entries, 1, "no output may be opened before confirmation")
				data, err := os.ReadFile(existing)
				require.NoError(t, err)
				assert.Equal(t, "keep me", string(data))
			})
		}
	}
}

func TestAppendPreflightRepairsAllFilesAfterConfirmation(t *testing.T) {
	for _, tt := range []struct {
		name     string
		answer   string
		enabled  bool
		repaired bool
		wantErr  string
	}{
		{name: "accepted", answer: "yes\n", enabled: true, repaired: true},
		{name: "declined", answer: "no\n", enabled: true, wantErr: "recording cancelled"},
		{name: "validation does not inspect or repair", enabled: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			prefix := recordingTestFile(t,
				replay.Record{TimestampUnixNano: 1, Payload: []byte("a")},
				replay.Record{TimestampUnixNano: 2, Payload: []byte("bc")},
			)
			tails := map[string][]byte{
				"logs":    {0, 0},
				"metrics": {0, 0, 0, 13, 'd'},
				"traces":  nil,
			}
			exporters := make(map[string]any)
			for signal, tail := range tails {
				path := filepath.Join(dir, signal+".pb")
				require.NoError(t, os.WriteFile(path, append(append([]byte{}, prefix...), tail...), 0o600))
				exporters["file/"+signal] = map[string]any{"path": path, "format": "proto"}
			}
			appendExisting, overwrite := true, false
			var output bytes.Buffer
			p := recordingPreflight{
				enabled: &tt.enabled, appendExisting: &appendExisting, overwrite: &overwrite,
				input:  func() io.Reader { return strings.NewReader(tt.answer) },
				output: func() io.Writer { return &output },
			}
			err := p.Convert(t.Context(), confmap.NewFromStringMap(map[string]any{"exporters": exporters}))
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			for signal, tail := range tails {
				path := filepath.Join(dir, signal+".pb")
				data, readErr := os.ReadFile(path)
				require.NoError(t, readErr)
				expected := append([]byte{}, prefix...)
				if !tt.repaired {
					expected = append(expected, tail...)
				}
				assert.Equal(t, expected, data, signal)
				if tt.repaired && len(tail) > 0 {
					assert.Contains(t, output.String(), "removing incomplete trailing frame from "+strconv.Quote(path))
				}
			}
			if !tt.enabled {
				assert.Empty(t, output.String())
			}
		})
	}
}

func TestRecordingPreflight(t *testing.T) {
	dir := t.TempDir()
	existing := filepath.Join(dir, "existing.pb")
	original := recordingTestFile(t, replay.Record{TimestampUnixNano: 1, Payload: []byte("abc")})
	require.NoError(t, os.WriteFile(existing, original, 0o600))
	for _, tt := range []struct {
		name           string
		exporters      map[string]any
		appendExisting bool
		overwrite      bool
		answer         string
		wantErr        string
		warning        bool
	}{
		{
			name: "fresh path",
			exporters: map[string]any{
				"file/logs": map[string]any{"path": filepath.Join(dir, "fresh.pb")},
			},
		},
		{
			name: "fresh append path does not prompt",
			exporters: map[string]any{
				"file/logs": map[string]any{"path": filepath.Join(dir, "fresh.pb")},
			},
			appendExisting: true,
		},
		{
			name: "fresh overwrite path does not prompt",
			exporters: map[string]any{
				"file/logs": map[string]any{"path": filepath.Join(dir, "fresh.pb"), "append": true},
			},
			overwrite: true,
		},
		{
			name: "existing absolute path",
			exporters: map[string]any{
				"file/logs": map[string]any{"path": existing},
			},
			wantErr: "refusing to start",
		},
		{
			name: "confirmed overwrite overrides append config without touching file during preflight",
			exporters: map[string]any{
				"file/logs": map[string]any{"path": existing, "append": true},
			},
			overwrite: true,
			answer:    "yes\n",
			warning:   true,
		},
		{
			name: "confirmed append overrides truncate config",
			exporters: map[string]any{
				"file/logs": map[string]any{"path": existing, "format": "proto", "append": false},
			},
			appendExisting: true,
			answer:         "yes\n",
			warning:        true,
		},
		{
			name: "overwrite declined",
			exporters: map[string]any{
				"file/logs": map[string]any{"path": existing},
			},
			overwrite: true,
			answer:    "no\n",
			wantErr:   "recording cancelled",
			warning:   true,
		},
		{
			name: "append declined",
			exporters: map[string]any{
				"file/logs": map[string]any{"path": existing, "format": "proto"},
			},
			appendExisting: true,
			answer:         "\n",
			wantErr:        "recording cancelled",
			warning:        true,
		},
		{
			name: "overwrite without input",
			exporters: map[string]any{
				"file/logs": map[string]any{"path": existing},
			},
			overwrite: true,
			wantErr:   "explicit confirmation was not received",
			warning:   true,
		},
		{
			name: "append without input",
			exporters: map[string]any{
				"file/logs": map[string]any{"path": existing, "format": "proto"},
			},
			appendExisting: true,
			wantErr:        "explicit confirmation was not received",
			warning:        true,
		},
		{
			name: "unterminated confirmation is not accepted",
			exporters: map[string]any{
				"file/logs": map[string]any{"path": existing},
			},
			overwrite: true,
			answer:    "yes",
			wantErr:   "explicit confirmation was not received",
			warning:   true,
		},
		{
			name: "append config alone does not grant permission",
			exporters: map[string]any{
				"file/logs": map[string]any{"path": existing, "append": true},
			},
			wantErr: "refusing to start",
		},
		{
			name: "append refuses JSON instead of truncating it",
			exporters: map[string]any{
				"file/logs": map[string]any{"path": existing, "format": "json"},
			},
			appendExisting: true,
			answer:         "yes\n",
			wantErr:        "versioned recordings require format: proto",
		},
		{
			name: "append refuses compression",
			exporters: map[string]any{
				"file/logs": map[string]any{"path": existing, "format": "proto", "compression": "zstd"},
			},
			appendExisting: true,
			answer:         "yes\n",
			wantErr:        "without compression, an encoding extension, or rotation",
		},
		{
			name: "append refuses encoding extensions",
			exporters: map[string]any{
				"file/logs": map[string]any{"path": existing, "format": "proto", "encoding": "custom"},
			},
			appendExisting: true,
			answer:         "yes\n",
			wantErr:        "without compression, an encoding extension, or rotation",
		},
		{
			name: "append refuses directories",
			exporters: map[string]any{
				"file/logs": map[string]any{"path": dir, "format": "proto"},
			},
			appendExisting: true,
			answer:         "yes\n",
			wantErr:        "recording must be a regular file",
		},
		{
			name: "duplicate paths",
			exporters: map[string]any{
				"file/logs":   map[string]any{"path": filepath.Join(dir, "fresh.pb")},
				"file/traces": map[string]any{"path": filepath.Join(dir, "fresh.pb")},
			},
			wantErr: "share recording path",
		},
		{
			name: "dynamic paths",
			exporters: map[string]any{
				"file/logs": map[string]any{"path": "logs/*.pb", "group_by": map[string]any{"enabled": true}},
			},
			wantErr: "does not support group_by",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, value := range tt.exporters {
				exporter := value.(map[string]any)
				if _, exists := exporter["format"]; !exists {
					exporter["format"] = "proto"
				}
			}
			var output bytes.Buffer
			enabled := true
			p := recordingPreflight{
				enabled: &enabled, appendExisting: &tt.appendExisting, overwrite: &tt.overwrite,
				input:  func() io.Reader { return strings.NewReader(tt.answer) },
				output: func() io.Writer { return &output },
			}

			conf := confmap.NewFromStringMap(map[string]any{"exporters": tt.exporters})
			err := p.Convert(t.Context(), conf)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				if tt.appendExisting || tt.overwrite {
					assert.Equal(t, tt.appendExisting, conf.Get("exporters::file/logs::append"))
				}
			}
			assert.Equal(t, tt.warning, output.Len() > 0)
			if tt.warning {
				assert.Contains(t, output.String(), existing)
				assert.Contains(t, output.String(), "Type yes")
			}
			data, readErr := os.ReadFile(existing)
			require.NoError(t, readErr)
			assert.Equal(t, original, data)
		})
	}
}

func TestAppendRejectsInvalidFileHeaderWithoutModification(t *testing.T) {
	header := recordingTestFile(t)
	for _, data := range [][]byte{
		{0, 0, 0, 4, 'o', 'l', 'd', '!'},
		header[:len(header)-1],
		bytes.Replace(header, []byte(`"version":1`), []byte(`"version":9`), 1),
	} {
		dir := t.TempDir()
		path := filepath.Join(dir, "logs.pb")
		require.NoError(t, os.WriteFile(path, data, 0o600))
		enabled, appendExisting, overwrite := true, true, false
		p := recordingPreflight{
			enabled: &enabled, appendExisting: &appendExisting, overwrite: &overwrite,
			input:  func() io.Reader { return strings.NewReader("yes\n") },
			output: func() io.Writer { return io.Discard },
		}
		conf := confmap.NewFromStringMap(map[string]any{"exporters": map[string]any{
			"file/logs": map[string]any{"path": path, "format": "proto"},
		}})
		require.ErrorContains(t, p.Convert(t.Context(), conf), "invalid recording file header")
		after, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, data, after, "invalid file headers must never be treated as incomplete records")
	}
}
