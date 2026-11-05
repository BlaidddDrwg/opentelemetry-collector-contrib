// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"io"
	"sync"
	"time"

	replay "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/recordreplay"
)

const defaultProgressInterval = 5 * time.Second

type progressWriter struct {
	mu     *sync.Mutex
	writer io.Writer
}

func (w progressWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writer.Write(data)
}

func progressWriters(output, warnings io.Writer) (io.Writer, io.Writer) {
	mu := &sync.Mutex{}
	return progressWriter{mu: mu, writer: output}, progressWriter{mu: mu, writer: warnings}
}

type sessionProgress struct {
	mu        sync.Mutex
	mode      string
	output    io.Writer
	interval  time.Duration
	startOnce sync.Once
	started   time.Time
	stop      chan struct{}
	done      chan struct{}
	state     string
	stateAt   time.Time
	batches   uint64
	bytes     uint64
	last      time.Time
	record    uint64
	timestamp int64
	offset    time.Duration
	deadline  time.Time
	backwards uint64
	maxLate   time.Duration
}

func newSessionProgress(mode string, output io.Writer, interval time.Duration) *sessionProgress {
	return &sessionProgress{
		mode: mode, output: output, interval: interval,
		stop: make(chan struct{}), done: make(chan struct{}),
	}
}

func (p *sessionProgress) start(details string) {
	p.startOnce.Do(func() {
		p.mu.Lock()
		p.started = time.Now()
		p.stateAt = p.started
		p.state = "waiting_for_data"
		if p.mode == "Replay" {
			p.state = "connecting"
		}
		p.mu.Unlock()
		_, _ = fmt.Fprintf(p.output, "%s started: %s progress_interval=%s\n", p.mode, details, p.interval)
		if p.interval == 0 {
			close(p.done)
			return
		}
		go func() {
			defer close(p.done)
			ticker := time.NewTicker(p.interval)
			defer ticker.Stop()
			for {
				select {
				case <-p.stop:
					return
				case <-ticker.C:
					_ = p.report("")
				}
			}
		}()
	})
}

func (p *sessionProgress) recorded(size int) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.batches++
	p.bytes += uint64(size)
	p.last = time.Now()
	p.state = "recording"
	p.stateAt = p.last
}

func (p *sessionProgress) replayProgress(update replay.Progress) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state, p.stateAt = update.State, time.Now()
	p.record, p.timestamp, p.offset, p.deadline = update.Record, update.TimestampUnixNano, update.Offset, update.Deadline
	if update.Stats.Sent > p.batches {
		p.last = time.Now()
		p.bytes = update.Stats.PayloadBytes
	}
	p.batches, p.backwards, p.maxLate = update.Stats.Sent, update.Stats.Backwards, update.Stats.MaxLate
}

func (p *sessionProgress) finish(runErr error) error {
	if p.started.IsZero() {
		return nil
	}
	close(p.stop)
	<-p.done
	status := "complete"
	if runErr != nil {
		status = "stopped"
	}
	return p.report(status)
}

func (p *sessionProgress) report(status string) error {
	p.mu.Lock()
	now := time.Now()
	elapsed := now.Sub(p.started)
	lastSuccess := "none"
	if !p.last.IsZero() {
		lastSuccess = now.Sub(p.last).Round(time.Millisecond).String()
	}
	rate := float64(p.batches) / elapsed.Seconds()
	byteRate := float64(p.bytes) / elapsed.Seconds()
	var line string
	if p.mode == "Recording" {
		label := "Recording progress"
		if status != "" {
			label = "Recording: status=" + status
		}
		line = fmt.Sprintf("%s elapsed=%s state=%s batches=%d payload_bytes=%d batches_per_sec=%.1f bytes_per_sec=%.1f last_success_age=%s",
			label, elapsed.Round(time.Millisecond), p.state, p.batches, p.bytes, rate, byteRate, lastSuccess)
	} else {
		label := "Replay progress:"
		if status != "" {
			label = "Replay:"
		}
		line = fmt.Sprintf("%s sent=%d backwards=%d max_lateness=%s elapsed=%s state=%s state_for=%s payload_bytes=%d batches_per_sec=%.1f bytes_per_sec=%.1f last_success_age=%s",
			label, p.batches, p.backwards, p.maxLate, elapsed.Round(time.Millisecond), p.state,
			now.Sub(p.stateAt).Round(time.Millisecond), p.bytes, rate, byteRate, lastSuccess)
		if p.record != 0 && status != "complete" {
			line += fmt.Sprintf(" record=%d", p.record)
			if p.state != "reading" {
				wait := max(time.Duration(0), p.deadline.Sub(now))
				line += fmt.Sprintf(" recorded_time=%s offset=%s wait_remaining=%s",
					time.Unix(0, p.timestamp).UTC().Format(time.RFC3339Nano), p.offset, wait.Round(time.Millisecond))
			}
		}
		if status != "" {
			line += " status=" + status
		}
	}
	p.mu.Unlock()
	_, err := fmt.Fprintln(p.output, line)
	return err
}
