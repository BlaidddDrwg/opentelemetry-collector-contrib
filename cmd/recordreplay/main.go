// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main // import "github.com/open-telemetry/opentelemetry-collector-contrib/cmd/recordreplay"

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/confmap/provider/yamlprovider"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/fileexporter"
	replay "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/recordreplay"
)

func main() {
	if err := newCommand().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newCommand(loggingOptions ...zap.Option) *cobra.Command {
	root := &cobra.Command{
		Use:           "recordreplay",
		Short:         "Record and replay timestamped OTLP telemetry",
		SilenceErrors: true,
		SilenceUsage:  true,
	}
	var appendExisting, overwrite, checkFiles bool
	var progressInterval time.Duration
	var progress *sessionProgress
	var stopCapture func() error
	var recordingContext context.Context
	var cancelRecording context.CancelFunc
	root.PersistentFlags().DurationVar(&progressInterval, "progress-interval", defaultProgressInterval, "Progress update interval (0 disables periodic updates)")

	record := otelcol.NewCommand(otelcol.CollectorSettings{
		BuildInfo: component.BuildInfo{
			Command:     "recordreplay",
			Description: "Standalone OTLP recorder",
			Version:     "development",
		},
		Factories: func() (otelcol.Factories, error) {
			return recordingComponents(progress)
		},
		LoggingOptions: loggingOptions,
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				ProviderFactories: []confmap.ProviderFactory{
					fileprovider.NewFactory(),
					envprovider.NewFactory(),
					yamlprovider.NewFactory(),
				},
				ConverterFactories: []confmap.ConverterFactory{
					confmap.NewConverterFactory(func(confmap.ConverterSettings) confmap.Converter {
						return recordingPreflight{
							enabled:        &checkFiles,
							appendExisting: &appendExisting,
							overwrite:      &overwrite,
							input:          root.InOrStdin,
							output:         root.ErrOrStderr,
							ready: func(paths map[string]string, duration time.Duration) error {
								if stopCapture != nil {
									return nil
								}
								var outputs []string
								for path, id := range paths {
									outputs = append(outputs, id+"="+path)
								}
								sort.Strings(outputs)
								progress.start(fmt.Sprintf("files=%q duration=%s counters=current-session buffered-writes", outputs, duration))
								stopCapture = startCaptureTimer(recordingContext, duration, root.OutOrStdout(), cancelRecording)
								return nil
							},
						}
					}),
				},
			},
		},
	})
	record.Use = "record"
	record.Short = "Receive OTLP telemetry and write it to configured files"
	record.Args = cobra.NoArgs
	record.PersistentFlags().BoolVar(&appendExisting, "append", false, "Append to recording files; existing files require confirmation")
	record.PersistentFlags().BoolVar(&overwrite, "overwrite", false, "Truncate recording files; existing files require confirmation")
	record.MarkFlagsMutuallyExclusive("append", "overwrite")
	runRecord := record.RunE
	record.RunE = func(cmd *cobra.Command, args []string) error {
		if progressInterval < 0 {
			return errors.New("progress-interval must not be negative")
		}
		output, warnings := progressWriters(root.OutOrStdout(), root.ErrOrStderr())
		root.SetOut(output)
		root.SetErr(warnings)
		progress = newSessionProgress("Recording", output, progressInterval)
		stopCapture = nil
		recordingContext, cancelRecording = context.WithCancel(cmd.Context())
		defer cancelRecording()
		cmd.SetContext(recordingContext)
		checkFiles = true
		defer func() { checkFiles = false }()
		runErr := runRecord(cmd, args)
		if stopCapture != nil {
			runErr = errors.Join(runErr, stopCapture())
		}
		return errors.Join(runErr, progress.finish(runErr))
	}

	root.AddCommand(record, newReplayCommand(&progressInterval))
	return root
}

type recordingPreflight struct {
	enabled        *bool
	appendExisting *bool
	overwrite      *bool
	input          func() io.Reader
	output         func() io.Writer
	ready          func(map[string]string, time.Duration) error
}

func (p recordingPreflight) Convert(_ context.Context, conf *confmap.Conf) (err error) {
	duration, err := recordingDuration(conf)
	if err != nil {
		return err
	}
	paths := make(map[string]string)
	defer func() {
		if err == nil && *p.enabled && p.ready != nil {
			err = p.ready(paths, duration)
		}
	}()

	// Because we're going to process overrides below, and this is a required.
	conf.Delete("replay")
	conf.Delete("record")

	exporters, err := conf.Sub("exporters")
	if err != nil {
		return err
	}
	var existing []string
	overrides := make(map[string]any)
	for id := range exporters.ToStringMap() {
		if strings.SplitN(id, "/", 2)[0] != "file" {
			continue
		}
		sub, subErr := exporters.Sub(id)
		if subErr != nil {
			return subErr
		}
		cfg := fileexporter.NewFactory().CreateDefaultConfig().(*fileexporter.Config)
		if unmarshalErr := sub.Unmarshal(cfg); unmarshalErr != nil {
			return unmarshalErr
		}
		if *p.appendExisting || *p.overwrite {
			cfg.Append = *p.appendExisting
			overrides[id] = map[string]any{"append": cfg.Append}
		}
		if validateErr := cfg.Validate(); validateErr != nil {
			return validateErr
		}
		if cfg.FormatType != "proto" || cfg.Compression != "" || cfg.Encoding != nil || cfg.Rotation != nil {
			return errors.New("versioned recordings require format: proto without compression, an encoding extension, or rotation")
		}
		if !*p.enabled {
			continue
		}
		if cfg.GroupBy != nil && cfg.GroupBy.Enabled {
			return errors.New("record does not support group_by: output files must be known before startup")
		}
		path, pathErr := filepath.Abs(cfg.Path)
		if pathErr != nil {
			return pathErr
		}
		if previous, ok := paths[path]; ok {
			return fmt.Errorf("exporters %s and %s share recording path %q; use separate files", previous, id, path)
		}
		paths[path] = id
		if info, statErr := os.Lstat(path); statErr == nil {
			if *p.appendExisting {
				if !info.Mode().IsRegular() {
					return fmt.Errorf("cannot safely append to %q: recording must be a regular file, not a symlink or special file", path)
				}
			}
			existing = append(existing, path)
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return fmt.Errorf("cannot check recording path %q: %w", path, statErr)
		}
	}
	if len(overrides) > 0 {
		if mergeErr := conf.Merge(confmap.NewFromStringMap(map[string]any{"exporters": overrides})); mergeErr != nil {
			return mergeErr
		}
	}
	if !*p.enabled || len(existing) == 0 {
		return nil
	}
	sort.Strings(existing)
	if !*p.appendExisting && !*p.overwrite {
		return fmt.Errorf("recording files already exist:\n%s\nrefusing to start; choose fresh paths, --append to retain existing data, or --overwrite to truncate it; modifying existing files requires confirmation", strings.Join(existing, "\n"))
	}
	mode := "--overwrite will TRUNCATE these files and erase their existing contents"
	if *p.appendExisting {
		mode = "--append will retain complete records, remove any incomplete trailing frame, and add new data"
	}
	if _, err = fmt.Fprintf(p.output(), "WARNING: %s:\n%s\nStop any other recorder writing these files before proceeding.\nType yes and press Enter to modify the existing files: ", mode, strings.Join(existing, "\n")); err != nil {
		return fmt.Errorf("cannot display recording confirmation: %w", err)
	}
	answer, err := bufio.NewReader(p.input()).ReadString('\n')
	if err != nil {
		return fmt.Errorf("recording cancelled: explicit confirmation was not received: %w", err)
	}
	if !strings.EqualFold(strings.TrimSpace(answer), "yes") {
		return errors.New("recording cancelled: existing files were not modified")
	}
	if *p.appendExisting {
		for _, path := range existing {
			file, openErr := os.OpenFile(path, os.O_RDWR, 0)
			if openErr != nil {
				return fmt.Errorf("cannot check recording %q before append: %w", path, openErr)
			}
			validBytes, checkErr := replay.VerifyRecording(file)
			if errors.Is(checkErr, io.ErrUnexpectedEOF) && !errors.Is(checkErr, replay.ErrInvalidHeader) {
				if _, checkErr = fmt.Fprintf(p.output(), "WARNING: removing incomplete trailing frame from %q; preserving the first %d bytes\n", path, validBytes); checkErr == nil {
					checkErr = replay.TruncateIncompleteRecording(file, validBytes)
				}
			}
			if err = errors.Join(checkErr, file.Close()); err != nil {
				return fmt.Errorf("cannot prepare recording %q for append: %w", path, err)
			}
		}
	}
	return nil
}

func recordingComponents(progress *sessionProgress) (otelcol.Factories, error) {
	receivers, err := otelcol.MakeFactoryMap[receiver.Factory](otlpreceiver.NewFactory())
	if err != nil {
		return otelcol.Factories{}, err
	}
	exporters, err := otelcol.MakeFactoryMap[exporter.Factory](fileexporter.NewFactoryWithFileFormat(replay.WriteHeader, func(writer io.Writer, payload []byte) error {
		if writeErr := replay.WriteBatch(writer, payload); writeErr != nil {
			return writeErr
		}
		progress.recorded(len(payload))
		return nil
	}))
	if err != nil {
		return otelcol.Factories{}, err
	}
	return otelcol.Factories{
		Receivers: receivers,
		Exporters: exporters,
		Telemetry: otelconftelemetry.NewFactory(),
	}, nil
}
