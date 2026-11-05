// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	replay "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/recordreplay"
)

func newReplayCommand(progressInterval *time.Duration) *cobra.Command {
	var paths, overrides []string
	cmd := &cobra.Command{
		Use:   "replay [file]",
		Short: "Replay a recording using offsets from its first timestamp",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if *progressInterval < 0 {
				return errors.New("progress-interval must not be negative")
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			file := ""
			if len(args) > 0 {
				file = args[0]
			}
			cfg, err := loadReplayConfig(ctx, paths, overrides, file)
			if err != nil {
				return err
			}
			return replayFile(ctx, cfg, cmd.ErrOrStderr(), cmd.OutOrStdout(), *progressInterval)
		},
	}
	cmd.Flags().StringSliceVar(&paths, "config", []string{"config.yaml"}, "Shared configuration file(s) or provider URI(s)")
	cmd.Flags().StringArrayVar(&overrides, "set", nil, "Override replay configuration: replay.<field>=<value>")
	return cmd
}

func replayFile(ctx context.Context, cfg replay.Config, warnings, output io.Writer, progressInterval time.Duration) (err error) {
	output, warnings = progressWriters(output, warnings)
	file, size, err := replay.OpenInput(cfg.File)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	// The standalone command requires a versioned header, even for no records.
	if size == 0 {
		return fmt.Errorf("cannot read recording header: %w", io.EOF)
	}
	reader, err := replay.NewReader(file, size)
	if err != nil {
		return err
	}
	first, err := replay.ReadFrame(reader, cfg.MaxPayloadSize)
	if errors.Is(err, io.EOF) {
		_, err = fmt.Fprintln(output, "Replay complete: 0 records (header-only recording)")
		return err
	}
	if err != nil {
		return fmt.Errorf("cannot read first recording record: %w", err)
	}
	progress := newSessionProgress("Replay", output, progressInterval)
	progress.start(fmt.Sprintf("file=%q input_bytes=%d first_recorded_time=%s signal=%s protocol=%s endpoint=%q tls=%t server_name=%q rebase_timestamps=%t lateness_tolerance=%s timeout=%s",
		cfg.File, size, time.Unix(0, first.TimestampUnixNano).UTC().Format(time.RFC3339Nano),
		cfg.Signal, cfg.Protocol, cfg.Endpoint, !cfg.TLS.Insecure, cfg.TLS.ServerName, cfg.RebaseTimestamps, cfg.LatenessTolerance, cfg.Timeout))
	if cfg.RebaseTimestamps {
		_, _ = fmt.Fprintln(output, "Replay note: payload timestamps are shifted to the current session; original timing relationships and the source file are preserved.")
	} else {
		_, _ = fmt.Fprintln(output, "Replay note: payload event timestamps are unchanged; historical data may be outside the graph's current time range.")
	}
	defer func() { err = errors.Join(err, progress.finish(err)) }()
	sender, err := replay.NewSender(ctx, cfg, false)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, sender.Close()) }()
	_, err = replay.Replay(ctx, reader, first, cfg, sender.Prepare, replay.Clock{}, nil, warnings, progress.replayProgress)
	return err
}
