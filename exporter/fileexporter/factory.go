// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package fileexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/fileexporter"

import (
	"context"
	"errors"
	"io"
	"os"
	"time"

	"github.com/DeRuina/timberjack"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
	"go.opentelemetry.io/collector/exporter/xexporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/fileexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/sharedcomponent"
)

const (
	// the number of old log files to retain
	defaultMaxBackups = 100

	// the format of encoded telemetry data
	formatTypeJSON  = "json"
	formatTypeProto = "proto"

	// the type of compression codec
	compressionZSTD = "zstd"

	defaultMaxOpenFiles = 100

	defaultResourceAttribute = "fileexporter.path_segment"

	// Permissions used when creating new export files.
	defaultFilePermissions = 0o644
)

type FileExporter interface {
	component.Component
	consumeTraces(_ context.Context, td ptrace.Traces) error
	consumeMetrics(_ context.Context, md pmetric.Metrics) error
	consumeLogs(_ context.Context, ld plog.Logs) error
	consumeProfiles(_ context.Context, pd pprofile.Profiles) error
}

// NewFactory creates a factory for OTLP exporter.
func NewFactory() exporter.Factory {
	return NewFactoryWithFrameWriter(nil)
}

type factory struct {
	format    *fileFormat
	exporters *sharedcomponent.SharedComponents
}

type fileFormat struct {
	writeHeader func(io.Writer) error
	writeFrame  func(io.Writer, []byte) error
}

func (f *fileFormat) validate(rotation *Rotation) error {
	if f != nil && f.writeHeader != nil && rotation != nil {
		return errors.New("custom file headers cannot be combined with rotation")
	}
	return nil
}

// NewFactoryWithFrameWriter overrides length-prefixed message writing.
// The callback receives encoded payloads and runs under the file writer's lock.
// It must write the complete frame without retaining the writer or payload.
// A nil callback preserves the default framing. Newline-delimited output is unaffected.
func NewFactoryWithFrameWriter(writeFrame func(io.Writer, []byte) error) exporter.Factory {
	return NewFactoryWithFileFormat(nil, writeFrame)
}

// NewFactoryWithFileFormat customizes file-header and length-prefixed frame writing.
// The header is written once when opening an empty file, including after truncation,
// but not when appending to a nonempty file. A header callback disables rotation.
// Callbacks must not retain their arguments or close the writer.
// Nil callbacks preserve the corresponding default behavior.
func NewFactoryWithFileFormat(writeHeader func(io.Writer) error, writeFrame func(io.Writer, []byte) error) exporter.Factory {
	f := &factory{
		format:    &fileFormat{writeHeader: writeHeader, writeFrame: writeFrame},
		exporters: sharedcomponent.NewSharedComponents(),
	}
	return xexporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		xexporter.WithTraces(f.createTracesExporter, metadata.TracesStability),
		xexporter.WithMetrics(f.createMetricsExporter, metadata.MetricsStability),
		xexporter.WithLogs(f.createLogsExporter, metadata.LogsStability),
		xexporter.WithProfiles(f.createProfilesExporter, metadata.ProfilesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		FormatType: formatTypeJSON,
		Rotation:   &Rotation{MaxBackups: defaultMaxBackups},
		GroupBy: &GroupBy{
			ResourceAttribute: defaultResourceAttribute,
			MaxOpenFiles:      defaultMaxOpenFiles,
		},
	}
}

func (f *factory) createTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	fe := f.getOrCreateFileExporter(cfg, set.Logger)
	return exporterhelper.NewTraces(
		ctx,
		set,
		cfg,
		fe.consumeTraces,
		exporterhelper.WithStart(fe.Start),
		exporterhelper.WithShutdown(fe.Shutdown),
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
	)
}

func (f *factory) createMetricsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	fe := f.getOrCreateFileExporter(cfg, set.Logger)
	return exporterhelper.NewMetrics(
		ctx,
		set,
		cfg,
		fe.consumeMetrics,
		exporterhelper.WithStart(fe.Start),
		exporterhelper.WithShutdown(fe.Shutdown),
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
	)
}

func (f *factory) createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	fe := f.getOrCreateFileExporter(cfg, set.Logger)
	return exporterhelper.NewLogs(
		ctx,
		set,
		cfg,
		fe.consumeLogs,
		exporterhelper.WithStart(fe.Start),
		exporterhelper.WithShutdown(fe.Shutdown),
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
	)
}

func (f *factory) createProfilesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (xexporter.Profiles, error) {
	fe := f.getOrCreateFileExporter(cfg, set.Logger)
	return xexporterhelper.NewProfiles(
		ctx,
		set,
		cfg,
		fe.consumeProfiles,
		exporterhelper.WithStart(fe.Start),
		exporterhelper.WithShutdown(fe.Shutdown),
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
	)
}

// getOrCreateFileExporter creates a FileExporter and caches it for a particular configuration,
// or returns the already cached one. Caching is required because the factory is asked trace and
// metric receivers separately when it gets CreateTraces() and CreateMetrics()
// but they must not create separate objects, they must use one Exporter object per configuration.
func (f *factory) getOrCreateFileExporter(cfg component.Config, logger *zap.Logger) FileExporter {
	conf := cfg.(*Config)
	fe := f.exporters.GetOrAdd(cfg, func() component.Component {
		return newFileExporter(conf, logger, f.format)
	})

	c := fe.Unwrap()
	return c.(FileExporter)
}

func newFileExporter(conf *Config, logger *zap.Logger, format *fileFormat) FileExporter {
	if conf.GroupBy == nil || !conf.GroupBy.Enabled {
		return &fileExporter{
			conf:   conf,
			logger: logger,
			format: format,
		}
	}

	return &groupingFileExporter{
		conf:   conf,
		logger: logger,
		format: format,
	}
}

func newFileWriter(path string, shouldAppend bool, rotation *Rotation, flushInterval, syncInterval time.Duration, export exportFunc, compression string, compressionLevel int, format *fileFormat) (*fileWriter, error) {
	if err := format.validate(rotation); err != nil {
		return nil, err
	}
	var baseWriter io.WriteCloser
	var wc io.WriteCloser
	var syncFile func() error
	var initialize bool

	if rotation == nil {
		fileFlags := os.O_RDWR | os.O_CREATE
		if shouldAppend {
			fileFlags |= os.O_APPEND
		} else {
			fileFlags |= os.O_TRUNC
		}
		f, err := os.OpenFile(path, fileFlags, defaultFilePermissions)
		if err != nil {
			return nil, err
		}
		baseWriter = f
		if format != nil && format.writeHeader != nil {
			info, statErr := f.Stat()
			if statErr != nil {
				return nil, errors.Join(statErr, f.Close())
			}
			initialize = info.Size() == 0
		}
		if syncInterval > 0 {
			baseWriter = &syncingWriteCloser{file: f}
			syncFile = f.Sync
		}
	} else {
		baseWriter = &timberjack.Logger{
			Filename:    path,
			MaxSize:     rotation.MaxMegabytes,
			MaxAge:      rotation.MaxDays,
			MaxBackups:  rotation.MaxBackups,
			LocalTime:   rotation.LocalTime,
			FileMode:    defaultFilePermissions,
			Compression: "none", // ensure compression is handled by the collector
		}
	}

	switch {
	case compression != "" && metadata.ExporterFileNativeCompressionFeatureGate.IsEnabled():
		var err error
		wc, err = newCompressingWriter(baseWriter, compression, compressionLevel, rotation)
		if err != nil {
			baseWriter.Close()
			return nil, err
		}
	case rotation == nil:
		wc = newBufferedWriteCloser(baseWriter)
	default:
		wc = baseWriter
	}

	writer := &fileWriter{
		path:          path,
		file:          wc,
		exporter:      export,
		flushInterval: flushInterval,
		syncInterval:  syncInterval,
		syncFile:      syncFile,
	}
	if format != nil {
		writer.writeFrame = format.writeFrame
		if initialize {
			if err := format.writeHeader(wc); err != nil {
				return nil, errors.Join(err, wc.Close())
			}
		}
	}
	return writer, nil
}
