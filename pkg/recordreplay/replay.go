// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package replay provides bounded recording inspection and sequential OTLP replay.
// It does not own configuration providers, process lifetime, or presentation.
package replay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"time"
)

type PrepareFunc func([]byte, time.Duration) (func(context.Context) error, error)

type Clock struct {
	Now  func() time.Time
	Wait func(context.Context, time.Time) error
}

func WaitDeadline(ctx context.Context, deadline time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay := time.Until(deadline); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ctx.Err()
}

type Stats struct {
	Sent, Backwards                                               uint64
	MaxLate                                                       time.Duration
	Attempted, Accepted, Failed, Ambiguous, Batches, PayloadBytes uint64
}

type Progress struct {
	State             string
	Record            uint64
	TimestampUnixNano int64
	Offset            time.Duration
	Deadline          time.Time
	Stats             Stats
}

type Schedule struct {
	Start          time.Time
	OriginUnixNano int64
	TimestampShift time.Duration
}

// LatenessError reports a missed strict deadline before any export attempt.
type LatenessError struct {
	Record    uint64
	Late      time.Duration
	Tolerance time.Duration
}

func (e *LatenessError) Error() string {
	return fmt.Sprintf("record %d missed replay deadline by %s (tolerance %s); stopped before sending it", e.Record, e.Late, e.Tolerance)
}

// Replay consumes frames after first. A nil schedule starts after preparation
// and preserves standalone backwards-timestamp behavior. An explicit schedule
// uses the caller's common origin, epoch, and payload shift without relaxing
// deadlines for backwards records. Callback values are immutable snapshots.
func Replay(ctx context.Context, reader io.Reader, first Record, cfg Config, prepare PrepareFunc, clock Clock, schedule *Schedule, warnings io.Writer, onProgress func(Progress)) (Stats, error) {
	var stats Stats
	if clock.Now == nil {
		clock.Now = time.Now
	}
	if clock.Wait == nil {
		clock.Wait = WaitDeadline
	}
	if warnings == nil {
		warnings = io.Discard
	}
	record := first
	previous := first.TimestampUnixNano
	origin := first.TimestampUnixNano
	var start time.Time
	var shift time.Duration
	if schedule != nil {
		start, origin, shift = schedule.Start, schedule.OriginUnixNano, schedule.TimestampShift
	}
	number := uint64(1)
	emit := func(state string, offset time.Duration, deadline time.Time) {
		if onProgress != nil {
			onProgress(Progress{State: state, Record: number, TimestampUnixNano: record.TimestampUnixNano, Offset: offset, Deadline: deadline, Stats: stats})
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		emit("preparing", 0, time.Time{})
		if number == 1 && schedule == nil && cfg.RebaseTimestamps {
			var err error
			shift, err = TimestampShift(first.TimestampUnixNano, clock.Now().UnixNano())
			if err != nil {
				return stats, err
			}
		}
		items, err := PayloadItems(cfg.Signal, record.Payload)
		if err != nil {
			return stats, fmt.Errorf("record %d: invalid %s payload: %w", number, cfg.Signal, err)
		}
		send, err := prepare(record.Payload, shift)
		if err != nil {
			return stats, fmt.Errorf("record %d: invalid %s payload: %w", number, cfg.Signal, err)
		}
		if number == 1 && schedule == nil {
			start = clock.Now()
		}
		backwards := record.TimestampUnixNano < previous || record.TimestampUnixNano < first.TimestampUnixNano
		deadline := clock.Now()
		var offset time.Duration
		immediate := backwards && schedule == nil
		if immediate {
			stats.Backwards++
			if _, err = fmt.Fprintf(warnings, "WARNING: record %d timestamp %d moves backwards or precedes zero time; sending immediately in file order\n", number, record.TimestampUnixNano); err != nil {
				return stats, err
			}
			offset = deadline.Sub(start)
		} else {
			offset, err = TimestampShift(origin, record.TimestampUnixNano)
			if err != nil || offset < 0 {
				return stats, fmt.Errorf("record %d: timestamp precedes replay origin or offset exceeds time.Duration range", number)
			}
			if backwards {
				stats.Backwards++
			}
			deadline = start.Add(offset)
			emit("waiting", offset, deadline)
			if waitErr := clock.Wait(ctx, deadline); waitErr != nil {
				return stats, waitErr
			}
		}
		if contextErr := ctx.Err(); contextErr != nil {
			return stats, contextErr
		}
		late := clock.Now().Sub(deadline)
		stats.MaxLate = max(stats.MaxLate, late)
		if !immediate && late > cfg.LatenessTolerance {
			emit("waiting", offset, deadline)
			return stats, &LatenessError{Record: number, Late: late, Tolerance: cfg.LatenessTolerance}
		}
		if items > math.MaxUint64-stats.Attempted || stats.Batches == math.MaxUint64 || uint64(len(record.Payload)) > math.MaxUint64-stats.PayloadBytes {
			return stats, errors.New("replay counters exceed uint64 range")
		}
		stats.Attempted += items
		sendCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
		emit("sending", offset, deadline)
		err = send(sendCtx)
		cancel()
		if err == nil {
			stats.Sent++
			stats.Accepted += items
			stats.Batches++
			stats.PayloadBytes += uint64(len(record.Payload))
		} else {
			var partial *PartialError
			if errors.As(err, &partial) && partial.Rejected > 0 && uint64(partial.Rejected) <= items {
				stats.Accepted += items - uint64(partial.Rejected)
				stats.Failed += uint64(partial.Rejected)
				stats.Batches++
				stats.PayloadBytes += uint64(len(record.Payload))
			} else {
				stats.Ambiguous += items
			}
		}
		emit("sending", offset, deadline)
		if err != nil {
			return stats, fmt.Errorf("record %d export failed after %d successful sends: %w", number, stats.Sent, err)
		}
		previous = record.TimestampUnixNano
		number++
		emit("reading", offset, time.Time{})
		record, err = ReadFrame(reader, cfg.MaxPayloadSize)
		if errors.Is(err, io.EOF) {
			return stats, nil
		}
		if err != nil {
			return stats, fmt.Errorf("cannot read record %d after %d successful sends: %w", number, stats.Sent, err)
		}
	}
}
