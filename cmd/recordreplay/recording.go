// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"go.opentelemetry.io/collector/confmap"
)

const defaultCaptureDuration = 5 * time.Minute

func recordingDuration(conf *confmap.Conf) (time.Duration, error) {
	cfg := struct {
		Duration time.Duration `mapstructure:"duration"`
	}{Duration: defaultCaptureDuration}
	sub, err := conf.Sub("record")
	if err != nil {
		return 0, err
	}
	if err = sub.Unmarshal(&cfg); err != nil {
		return 0, fmt.Errorf("invalid record configuration: %w", err)
	}
	if cfg.Duration <= 0 {
		return 0, errors.New("record.duration must be positive; unlimited captures are not supported")
	}
	return cfg.Duration, nil
}

func startCaptureTimer(ctx context.Context, duration time.Duration, output io.Writer, cancel context.CancelFunc) func() error {
	stop := make(chan struct{})
	done := make(chan error, 1)
	timer := time.NewTimer(duration)
	go func() {
		defer timer.Stop()
		select {
		case <-ctx.Done():
			done <- nil
		case <-stop:
			done <- nil
		case <-timer.C:
			cancel()
			_, err := fmt.Fprintf(output, "Recording duration %s reached: stopping the receiver, flushing complete records, syncing when configured, and closing files; please wait for shutdown to finish.\n", duration)
			done <- err
		}
	}()
	var once sync.Once
	var stopErr error
	return func() error {
		once.Do(func() {
			close(stop)
			stopErr = <-done
		})
		return stopErr
	}
}
