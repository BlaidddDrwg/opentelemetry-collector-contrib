// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package fileexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/fileexporter"

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/fileexporter/internal/metadata"
)

// exportFunc defines how to export encoded telemetry data.
type exportFunc func(e *fileWriter, buf []byte) error

type fileWriter struct {
	path  string
	file  io.WriteCloser
	mutex sync.Mutex

	exporter   exportFunc
	writeFrame func(io.Writer, []byte) error

	flushInterval time.Duration
	syncInterval  time.Duration
	syncFile      func() error
	logger        *zap.Logger
	stopTicker    chan struct{}
	wg            sync.WaitGroup
	backgroundErr error
	// Protected by mutex
	refs    int  // number of active references to this writer
	evicted bool // true if the writer has been evicted from the LRU
}

func exportMessageAsLine(w *fileWriter, buf []byte) error {
	// Ensure only one write operation happens at a time.
	w.mutex.Lock()
	defer w.mutex.Unlock()
	if w.file == nil {
		return io.ErrClosedPipe
	}
	if w.backgroundErr != nil {
		return w.backgroundErr
	}
	if _, err := w.file.Write(buf); err != nil {
		return err
	}
	if _, err := io.WriteString(w.file, "\n"); err != nil {
		return err
	}
	return nil
}

func exportMessageAsBuffer(w *fileWriter, buf []byte) error {
	// Ensure only one write operation happens at a time.
	w.mutex.Lock()
	defer w.mutex.Unlock()
	if w.file == nil {
		return io.ErrClosedPipe
	}
	if w.backgroundErr != nil {
		return w.backgroundErr
	}
	if w.writeFrame != nil {
		return w.writeFrame(w.file, buf)
	}
	// write the size of each message before writing the message itself.  https://developers.google.com/protocol-buffers/docs/techniques
	// each encoded object is preceded by 4 bytes (an unsigned 32 bit integer)
	data := make([]byte, 4, 4+len(buf))
	binary.BigEndian.PutUint32(data, uint32(len(buf)))

	return binary.Write(w.file, binary.BigEndian, append(data, buf...))
}

func (w *fileWriter) export(buf []byte) error {
	return w.exporter(w, buf)
}

func (w *fileWriter) startFlusher() {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	if _, ok := w.file.(interface{ flush() error }); !ok && w.syncInterval == 0 {
		return
	}
	// Create the stop channel and capture it locally so the goroutine
	// always selects on the original channel value, even after shutdown
	// nils out w.stopTicker.
	stop := make(chan struct{})
	w.stopTicker = stop
	// Start the ticker.
	var flushC, syncC <-chan time.Time
	var flushTicker, syncTicker *time.Ticker
	if w.flushInterval > 0 {
		flushTicker = time.NewTicker(w.flushInterval)
		flushC = flushTicker.C
	}
	if w.syncInterval > 0 {
		syncTicker = time.NewTicker(w.syncInterval)
		syncC = syncTicker.C
	}
	w.wg.Go(func() {
		defer func() {
			if flushTicker != nil {
				flushTicker.Stop()
			}
			if syncTicker != nil {
				syncTicker.Stop()
			}
		}()
		for {
			select {
			case <-flushC:
				w.maintain(false)
			case <-syncC:
				w.maintain(true)
			case <-stop:
				return
			}
		}
	})
}

// Start starts the flush timer if set.
func (w *fileWriter) start() {
	if w.flushInterval > 0 || w.syncInterval > 0 {
		w.startFlusher()
	}
}

func (w *fileWriter) maintain(syncFile bool) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	// A tick may already be queued when shutdown closes the writer.
	if w.stopTicker == nil || w.backgroundErr != nil {
		return
	}
	var err error
	if ff, ok := w.file.(interface{ flush() error }); ok {
		err = ff.flush()
	}
	if err == nil && syncFile && w.syncFile != nil {
		err = w.syncFile()
	}
	if err != nil {
		w.backgroundErr = err
		if w.logger != nil {
			w.logger.Error("Failed to flush or sync recording file", zap.String("path", w.path), zap.Error(err))
		}
	}
}

// acquire increments the reference count for this writer.
func (w *fileWriter) acquire() {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.refs++
}

// release decrements the reference count. If the writer was evicted and this
// was the last reference, it shuts down the writer.
func (w *fileWriter) release() error {
	w.mutex.Lock()
	w.refs--
	handOff := w.refs == 0 && w.evicted
	w.mutex.Unlock()
	if !handOff {
		return nil
	}
	return w.shutdown()
}

// evict marks the writer as evicted and shuts it down if there are no more references.
func (w *fileWriter) evict() error {
	w.mutex.Lock()
	w.evicted = true
	idle := w.refs == 0
	w.mutex.Unlock()
	if !idle {
		return nil
	}
	return w.shutdown()
}

// shutdown stops the flusher and closes the file.
func (w *fileWriter) shutdown() error {
	// Snapshot and nil out stopTicker under the lock to prevent a double-close
	// if shutdown is called more than once.
	w.mutex.Lock()
	stopTicker := w.stopTicker
	w.stopTicker = nil
	w.mutex.Unlock()

	if stopTicker != nil {
		close(stopTicker)
		// Wait for the flusher goroutine to finish before closing the file.
		// Cannot hold w.mutex here because the goroutine acquires it on each tick.
		w.wg.Wait()
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()
	if w.file == nil {
		return nil
	}
	err := errors.Join(w.backgroundErr, w.file.Close())
	w.file = nil
	return err
}

func buildExportFunc(cfg *Config) func(w *fileWriter, buf []byte) error {
	if metadata.ExporterFileNativeCompressionFeatureGate.IsEnabled() && cfg.Compression != "" {
		// Native compression: the compression stream handles framing, so
		// JSON can use newline-delimited output (human-readable after decompression).
		// Proto still needs length-prefix for message boundary detection.
		// When a custom encoding extension is set, the actual wire format may be
		// binary regardless of FormatType, so length-prefix framing is required.
		if cfg.FormatType == formatTypeJSON && cfg.Encoding == nil {
			return exportMessageAsLine
		}
		return exportMessageAsBuffer
	}
	// Legacy behavior
	if cfg.FormatType == formatTypeProto {
		return exportMessageAsBuffer
	}
	// if the data format is JSON and needs to be compressed, telemetry data can't be written to file in JSON format.
	if cfg.FormatType == formatTypeJSON && cfg.Compression != "" {
		return exportMessageAsBuffer
	}
	return exportMessageAsLine
}
