// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package fileexporter

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DeRuina/timberjack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/exporter/xexporter"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/fileexporter/internal/metadata"
)

func TestFactoryFrameWriter(t *testing.T) {
	for _, format := range []string{formatTypeProto, formatTypeJSON} {
		for _, grouped := range []bool{false, true} {
			name := format + "/single"
			if grouped {
				name = format + "/grouped"
			}
			t.Run(name, func(t *testing.T) {
				calls := 0
				var encoded []byte
				f := NewFactoryWithFrameWriter(func(writer io.Writer, payload []byte) error {
					calls++
					encoded = bytes.Clone(payload)
					_, err := writer.Write(append([]byte("custom:"), payload...))
					return err
				})
				cfg := f.CreateDefaultConfig().(*Config)
				cfg.Rotation = nil
				cfg.FormatType = format
				cfg.Path = filepath.Join(t.TempDir(), "logs.pb")
				outputPath := cfg.Path
				if grouped {
					cfg.GroupBy.Enabled = true
					cfg.Path = filepath.Join(filepath.Dir(cfg.Path), "logs-*.pb")
					outputPath = filepath.Join(filepath.Dir(cfg.Path), "logs-one.pb")
				}
				logs := plog.NewLogs()
				resource := logs.ResourceLogs().AppendEmpty()
				resource.Resource().Attributes().PutStr(defaultResourceAttribute, "one")
				resource.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("recorded")
				exp, err := f.CreateLogs(t.Context(), exportertest.NewNopSettings(metadata.Type), cfg)
				require.NoError(t, err)
				require.NoError(t, exp.Start(t.Context(), componenttest.NewNopHost()))
				t.Cleanup(func() { assert.NoError(t, exp.Shutdown(t.Context())) })
				require.NoError(t, exp.ConsumeLogs(t.Context(), logs))
				require.NoError(t, exp.Shutdown(t.Context()))
				data, err := os.ReadFile(outputPath)
				require.NoError(t, err)
				if format == formatTypeProto {
					assert.Equal(t, 1, calls)
					want, marshalErr := (&plog.ProtoMarshaler{}).MarshalLogs(logs)
					require.NoError(t, marshalErr)
					assert.Equal(t, want, encoded, "hook must receive the unframed encoded batch")
					assert.Equal(t, append([]byte("custom:"), want...), data)
				} else {
					assert.Zero(t, calls, "the hook must not alter newline-delimited output")
					want, marshalErr := (&plog.JSONMarshaler{}).MarshalLogs(logs)
					require.NoError(t, marshalErr)
					assert.Equal(t, append(want, '\n'), data)
				}
			})
		}
	}
}

func TestFactoryFrameWriterErrorsAndIsolation(t *testing.T) {
	cfg := &Config{Path: tempFileName(t), FormatType: formatTypeProto}
	logs := plog.NewLogs()
	logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("recorded")
	firstErr := errors.New("first frame writer failed")
	secondErr := errors.New("second frame writer failed")
	var exports []func() error
	for _, writeErr := range []error{firstErr, secondErr} {
		f := NewFactoryWithFrameWriter(func(io.Writer, []byte) error { return writeErr })
		exp, err := f.CreateLogs(t.Context(), exportertest.NewNopSettings(metadata.Type), cfg)
		require.NoError(t, err)
		require.NoError(t, exp.Start(t.Context(), componenttest.NewNopHost()))
		t.Cleanup(func() { assert.NoError(t, exp.Shutdown(t.Context())) })
		exports = append(exports, func() error { return exp.ConsumeLogs(t.Context(), logs) })
	}
	require.ErrorIs(t, exports[0](), firstErr)
	require.ErrorIs(t, exports[1](), secondErr, "factories must not share a cached exporter with a different framing hook")
}

func TestFactoryFileHeaderLifecycle(t *testing.T) {
	for _, tt := range []struct {
		name       string
		initial    []byte
		exists     bool
		append     bool
		writeBatch bool
	}{
		{name: "new file with batch", writeBatch: true},
		{name: "new file without batches"},
		{name: "append creates missing file", append: true, writeBatch: true},
		{name: "append initializes empty file", exists: true, append: true},
		{name: "append preserves existing header", exists: true, initial: []byte("HEADERold"), append: true, writeBatch: true},
		{name: "overwrite replaces existing header", exists: true, initial: []byte("old contents"), writeBatch: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "recording.pb")
			if tt.exists {
				require.NoError(t, os.WriteFile(path, tt.initial, 0o600))
			}
			headerCalls := 0
			f := NewFactoryWithFileFormat(
				func(writer io.Writer) error {
					headerCalls++
					_, err := io.WriteString(writer, "HEADER")
					return err
				},
				func(writer io.Writer, _ []byte) error {
					_, err := io.WriteString(writer, "record")
					return err
				},
			)
			cfg := &Config{Path: path, FormatType: formatTypeProto, Append: tt.append}
			start := func() {
				exp, err := f.CreateLogs(t.Context(), exportertest.NewNopSettings(metadata.Type), cfg)
				require.NoError(t, err)
				require.NoError(t, exp.Start(t.Context(), componenttest.NewNopHost()))
				t.Cleanup(func() { assert.NoError(t, exp.Shutdown(t.Context())) })
				if tt.writeBatch {
					logs := plog.NewLogs()
					logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("batch")
					require.NoError(t, exp.ConsumeLogs(t.Context(), logs))
				}
				require.NoError(t, exp.Shutdown(t.Context()))
			}
			start()
			want := "HEADER"
			expectedHeaderCalls := 1
			if tt.append && len(tt.initial) > 0 {
				want = string(tt.initial)
				expectedHeaderCalls = 0
			}
			if tt.writeBatch {
				want += "record"
			}
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			assert.Equal(t, want, string(data))
			assert.Equal(t, expectedHeaderCalls, headerCalls)

			cfg.Append = true
			start()
			if tt.writeBatch {
				want += "record"
			}
			data, err = os.ReadFile(path)
			require.NoError(t, err)
			assert.Equal(t, want, string(data), "a restart must not duplicate the file header")
			assert.Equal(t, expectedHeaderCalls, headerCalls)
		})
	}
}

func TestFactoryFileHeaderFailures(t *testing.T) {
	headerErr := errors.New("header failed")
	cfg := &Config{Path: tempFileName(t), FormatType: formatTypeProto}
	f := NewFactoryWithFileFormat(func(io.Writer) error { return headerErr }, nil)
	exp, err := f.CreateLogs(t.Context(), exportertest.NewNopSettings(metadata.Type), cfg)
	require.NoError(t, err)
	require.ErrorIs(t, exp.Start(t.Context(), componenttest.NewNopHost()), headerErr)
	require.NoError(t, exp.Shutdown(t.Context()))

	original := []byte("preserve existing contents")
	require.NoError(t, os.WriteFile(cfg.Path, original, 0o600))
	cfg.Rotation = &Rotation{MaxMegabytes: 1}
	exp, err = f.CreateLogs(t.Context(), exportertest.NewNopSettings(metadata.Type), cfg)
	require.NoError(t, err)
	require.ErrorContains(t, exp.Start(t.Context(), componenttest.NewNopHost()), "custom file headers cannot be combined with rotation")
	require.NoError(t, exp.Shutdown(t.Context()))
	data, err := os.ReadFile(cfg.Path)
	require.NoError(t, err)
	assert.Equal(t, original, data)
}

func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig()
	assert.NotNil(t, cfg, "failed to create default config")
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
}

func TestCreateMetricsError(t *testing.T) {
	cfg := &Config{
		FormatType: formatTypeJSON,
	}
	e, err := NewFactory().CreateMetrics(
		t.Context(),
		exportertest.NewNopSettings(metadata.Type),
		cfg,
	)
	require.NoError(t, err)
	err = e.Start(t.Context(), componenttest.NewNopHost())
	assert.Error(t, err)
}

func TestCreateMetrics(t *testing.T) {
	cfg := &Config{
		FormatType: formatTypeJSON,
		Path:       tempFileName(t),
	}
	exp, err := NewFactory().CreateMetrics(
		t.Context(),
		exportertest.NewNopSettings(metadata.Type),
		cfg,
	)
	assert.NoError(t, err)
	require.NotNil(t, exp)
	assert.NoError(t, exp.Shutdown(t.Context()))
}

func TestCreateTraces(t *testing.T) {
	cfg := &Config{
		FormatType: formatTypeJSON,
		Path:       tempFileName(t),
	}
	exp, err := NewFactory().CreateTraces(
		t.Context(),
		exportertest.NewNopSettings(metadata.Type),
		cfg,
	)
	assert.NoError(t, err)
	require.NotNil(t, exp)
	assert.NoError(t, exp.Shutdown(t.Context()))
}

func TestCreateTracesError(t *testing.T) {
	cfg := &Config{
		FormatType: formatTypeJSON,
	}
	e, err := NewFactory().CreateTraces(
		t.Context(),
		exportertest.NewNopSettings(metadata.Type),
		cfg,
	)
	require.NoError(t, err)
	err = e.Start(t.Context(), componenttest.NewNopHost())
	assert.Error(t, err)
}

func TestCreateLogs(t *testing.T) {
	cfg := &Config{
		FormatType: formatTypeJSON,
		Path:       tempFileName(t),
	}
	exp, err := NewFactory().CreateLogs(
		t.Context(),
		exportertest.NewNopSettings(metadata.Type),
		cfg,
	)
	assert.NoError(t, err)
	require.NotNil(t, exp)
	assert.NoError(t, exp.Shutdown(t.Context()))
}

func TestCreateLogsError(t *testing.T) {
	cfg := &Config{
		FormatType: formatTypeJSON,
	}
	e, err := NewFactory().CreateLogs(
		t.Context(),
		exportertest.NewNopSettings(metadata.Type),
		cfg,
	)
	require.NoError(t, err)
	err = e.Start(t.Context(), componenttest.NewNopHost())
	assert.Error(t, err)
}

func TestCreateProfiles(t *testing.T) {
	cfg := &Config{
		FormatType: formatTypeJSON,
		Path:       tempFileName(t),
	}
	exp, err := NewFactory().(xexporter.Factory).CreateProfiles(
		t.Context(),
		exportertest.NewNopSettings(metadata.Type),
		cfg,
	)
	assert.NoError(t, err)
	require.NotNil(t, exp)
	assert.NoError(t, exp.Shutdown(t.Context()))
}

func TestCreateProfilesError(t *testing.T) {
	cfg := &Config{
		FormatType: formatTypeJSON,
	}
	e, err := NewFactory().(xexporter.Factory).CreateProfiles(
		t.Context(),
		exportertest.NewNopSettings(metadata.Type),
		cfg,
	)
	require.NoError(t, err)
	err = e.Start(t.Context(), componenttest.NewNopHost())
	assert.Error(t, err)
}

func TestNewFileWriter(t *testing.T) {
	type args struct {
		cfg *Config
	}
	tests := []struct {
		name     string
		args     args
		want     io.WriteCloser
		validate func(*testing.T, *fileWriter)
	}{
		{
			name: "single file",
			args: args{
				cfg: &Config{
					Path:          tempFileName(t),
					FlushInterval: 5 * time.Second,
				},
			},
			validate: func(t *testing.T, writer *fileWriter) {
				assert.Equal(t, 5*time.Second, writer.flushInterval)
				_, ok := writer.file.(*bufferedWriteCloser)
				assert.True(t, ok)
			},
		},
		{
			name: "rotation file",
			args: args{
				cfg: &Config{
					Path: tempFileName(t),
					Rotation: &Rotation{
						MaxBackups: defaultMaxBackups,
					},
				},
			},
			validate: func(t *testing.T, writer *fileWriter) {
				logger, ok := writer.file.(*timberjack.Logger)
				assert.True(t, ok)
				assert.Equal(t, defaultMaxBackups, logger.MaxBackups)
			},
		},
		{
			name: "rotation file with user's configuration",
			args: args{
				cfg: &Config{
					Path: tempFileName(t),
					Rotation: &Rotation{
						MaxMegabytes: 30,
						MaxDays:      100,
						MaxBackups:   3,
						LocalTime:    true,
					},
				},
			},
			validate: func(t *testing.T, writer *fileWriter) {
				logger, ok := writer.file.(*timberjack.Logger)
				assert.True(t, ok)
				assert.Equal(t, 3, logger.MaxBackups)
				assert.Equal(t, 30, logger.MaxSize)
				assert.Equal(t, 100, logger.MaxAge)
				assert.True(t, logger.LocalTime)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newFileWriter(tt.args.cfg.Path, tt.args.cfg.Append, tt.args.cfg.Rotation, tt.args.cfg.FlushInterval, tt.args.cfg.SyncInterval, nil, "", 0, nil)
			defer func() {
				assert.NoError(t, got.file.Close())
			}()
			assert.NoError(t, err)
			tt.validate(t, got)
		})
	}
}
