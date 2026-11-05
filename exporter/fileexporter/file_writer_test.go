// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package fileexporter

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestPeriodicSync(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "sync failure"
		}
		t.Run(name, func(t *testing.T) {
			var data bytes.Buffer
			var synced []byte
			syncErr := errors.New("sync failed")
			calls := 0
			w := &fileWriter{
				file:         newBufferedWriteCloser(&nopWriteCloser{w: &data}),
				exporter:     exportMessageAsLine,
				syncInterval: time.Millisecond,
				logger:       zap.NewNop(),
				syncFile: func() error {
					calls++
					synced = bytes.Clone(data.Bytes())
					if fail {
						return syncErr
					}
					return nil
				},
			}
			require.NoError(t, w.export([]byte("recorded")))
			w.start()
			t.Cleanup(func() {
				w.mutex.Lock()
				stopped := w.stopTicker == nil
				w.mutex.Unlock()
				if !stopped {
					assert.ErrorIs(t, w.shutdown(), w.backgroundErr)
				}
			})
			require.Eventually(t, func() bool {
				w.mutex.Lock()
				defer w.mutex.Unlock()
				return calls > 0
			}, time.Second, time.Millisecond)
			w.mutex.Lock()
			assert.Equal(t, "recorded\n", string(synced), "sync must run after flushing buffered bytes")
			w.mutex.Unlock()
			err := w.export([]byte("next"))
			if fail {
				require.ErrorIs(t, err, syncErr)
				require.ErrorIs(t, w.shutdown(), syncErr)
			} else {
				require.NoError(t, err)
				require.NoError(t, w.shutdown())
			}
			// A timer tick queued before shutdown must not access the closed file.
			w.maintain(true)
		})
	}
}

type syncTestFile struct {
	bytes.Buffer
	synced []byte
	closed bool
	err    error
}

func (f *syncTestFile) Sync() error {
	f.synced = bytes.Clone(f.Bytes())
	return f.err
}

func (f *syncTestFile) Close() error {
	f.closed = true
	return nil
}

func TestSyncOnClose(t *testing.T) {
	for _, fail := range []bool{false, true} {
		f := &syncTestFile{}
		if fail {
			f.err = errors.New("sync failed")
		}
		w := newBufferedWriteCloser(&syncingWriteCloser{file: f})
		_, err := w.Write([]byte("recorded"))
		require.NoError(t, err)
		err = w.Close()
		if fail {
			require.ErrorIs(t, err, f.err)
		} else {
			require.NoError(t, err)
		}
		assert.Equal(t, "recorded", string(f.synced))
		assert.True(t, f.closed, "close must run even when sync fails")
	}
}

func TestSyncFlushFailure(t *testing.T) {
	syncs := 0
	w := &fileWriter{
		file: newBufferedWriteCloser(&errorWriter{}),
		syncFile: func() error {
			syncs++
			return nil
		},
		stopTicker: make(chan struct{}),
	}
	_, err := w.file.Write([]byte("recorded"))
	require.NoError(t, err)
	w.maintain(true)
	assert.Error(t, w.backgroundErr)
	assert.Zero(t, syncs, "a failed buffer flush must not be reported as a successful durable sync")
	assert.Error(t, w.shutdown())
}

func TestFileWriterSyncConfigured(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs.pb")
	w, err := newFileWriter(path, false, nil, time.Hour, time.Millisecond, exportMessageAsBuffer, "", 0, nil)
	require.NoError(t, err)
	require.NotNil(t, w.syncFile)
	require.NoError(t, w.export([]byte("recorded")))
	w.start()
	t.Cleanup(func() {
		w.mutex.Lock()
		stopped := w.stopTicker == nil
		w.mutex.Unlock()
		if !stopped {
			assert.NoError(t, w.shutdown())
		}
	})
	require.Eventually(t, func() bool {
		data, readErr := os.ReadFile(path)
		return readErr == nil && bytes.Equal(data, append([]byte{0, 0, 0, 8}, []byte("recorded")...))
	}, time.Second, time.Millisecond, "sync interval must flush even when the flush interval is much longer")
	require.NoError(t, w.shutdown())
}
