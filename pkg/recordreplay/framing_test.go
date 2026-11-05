// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordingFrameCompatibility(t *testing.T) {
	frames := []byte{
		0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 1, 'a', 'b', 'c',
		0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 10, 255, 255, 255, 255, 255, 255, 255, 254, 'd', 'e',
	}
	records := []Record{
		{TimestampUnixNano: 1, Payload: []byte("abc")},
		{Payload: []byte{}},
		{TimestampUnixNano: -2, Payload: []byte("de")},
	}
	var written bytes.Buffer
	for _, record := range records {
		require.NoError(t, WriteFrame(&written, record))
	}
	assert.Equal(t, frames, written.Bytes())

	reader := bytes.NewReader(frames)
	for _, want := range records {
		record, err := ReadFrame(reader, 3)
		require.NoError(t, err)
		assert.Equal(t, want, record)
	}
	record, err := ReadFrame(reader, 3)
	require.ErrorIs(t, err, io.EOF)
	assert.Empty(t, record)
}

func TestReadRecordingFrameIncomplete(t *testing.T) {
	frame := []byte{0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 1, 'a', 'b', 'c'}
	for size := 1; size < len(frame); size++ {
		_, err := ReadFrame(bytes.NewReader(frame[:size]), 3)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF, "frame truncated at byte %d", size)
	}
	_, err := ReadFrame(bytes.NewReader([]byte{255, 255, 255, 255}), 1024)
	require.ErrorContains(t, err, "exceeds limit", "reject excessive lengths before allocating or reading a payload")
	for _, length := range []byte{0, 1, 7} {
		_, err = ReadFrame(bytes.NewReader([]byte{0, 0, 0, length}), 3)
		require.ErrorContains(t, err, "timestamp requires")
		require.NotErrorIs(t, err, io.ErrUnexpectedEOF, "invalid lengths are not recoverable incomplete tails")
	}
}

func TestReadRecordingFrameIOErrors(t *testing.T) {
	readErr := errors.New("read failed")
	for _, reader := range []io.Reader{
		iotest.ErrReader(readErr),
		io.MultiReader(bytes.NewReader([]byte{0, 0}), iotest.ErrReader(readErr)),
		io.MultiReader(bytes.NewReader([]byte{0, 0, 0, 11, 0}), iotest.ErrReader(readErr)),
		io.MultiReader(bytes.NewReader([]byte{0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 1, 'a'}), iotest.ErrReader(readErr)),
	} {
		_, err := ReadFrame(reader, 3)
		require.ErrorIs(t, err, readErr)
	}
}

type recordingTestWriter func([]byte) (int, error)

func (w recordingTestWriter) Write(data []byte) (int, error) {
	return w(data)
}

func TestWriteRecordingFrameErrors(t *testing.T) {
	writeErr := errors.New("write failed")
	for _, tt := range []struct {
		name string
		n    int
		err  error
		want error
	}{
		{name: "short write", n: 14, want: io.ErrShortWrite},
		{name: "no progress", want: io.ErrShortWrite},
		{name: "write error", err: writeErr, want: writeErr},
		{name: "partial write error", n: 14, err: writeErr, want: writeErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			err := WriteFrame(recordingTestWriter(func(frame []byte) (int, error) {
				calls++
				assert.Equal(t, []byte{0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 1, 'a', 'b', 'c'}, frame)
				return tt.n, tt.err
			}), Record{TimestampUnixNano: 1, Payload: []byte("abc")})
			require.ErrorIs(t, err, tt.want)
			assert.Equal(t, 1, calls, "length, timestamp, and payload must be submitted together")
		})
	}
}

func TestVerifyAndTruncateRecording(t *testing.T) {
	fileHeader := recordingTestFile(t)
	first := []byte{0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 1, 'a', 'b', 'c'}
	second := []byte{0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 2, 'd', 'e'}
	for _, tt := range []struct {
		name   string
		prefix []byte
		tail   []byte
	}{
		{name: "empty"},
		{name: "header only"},
		{name: "complete frame", prefix: first},
		{name: "multiple complete frames", prefix: append(append([]byte{}, first...), second...)},
		{name: "empty payload", prefix: []byte{0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0}},
		{name: "first header incomplete", tail: []byte{0, 0}},
		{name: "one byte header tail", prefix: first, tail: []byte{0}},
		{name: "two byte header tail", prefix: first, tail: []byte{0, 0}},
		{name: "three byte header tail", prefix: first, tail: []byte{0, 0, 0}},
		{name: "timestamp missing", prefix: first, tail: []byte{0, 0, 0, 11}},
		{name: "partial timestamp", prefix: first, tail: []byte{0, 0, 0, 11, 0, 0}},
		{name: "payload missing", prefix: first, tail: []byte{0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 1}},
		{name: "partial payload", prefix: first, tail: []byte{0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 1, 'd', 'e'}},
		{name: "first payload incomplete", tail: []byte{0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 1, 'a'}},
		{name: "maximum length without allocating payload", prefix: first, tail: []byte{255, 255, 255, 255}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "recording.pb")
			prefix := append(bytes.Clone(fileHeader), tt.prefix...)
			if tt.name == "empty" {
				prefix = []byte{}
			}
			original := append(bytes.Clone(prefix), tt.tail...)
			require.NoError(t, os.WriteFile(path, original, 0o600))
			file, err := os.OpenFile(path, os.O_RDWR, 0)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, file.Close()) })

			// Verification must work independently of the caller's file position.
			_, err = file.Seek(0, io.SeekEnd)
			require.NoError(t, err)
			validBytes, err := VerifyRecording(file)
			assert.Equal(t, int64(len(prefix)), validBytes)
			if len(tt.tail) > 0 {
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			} else {
				require.NoError(t, err)
			}
			position, err := file.Seek(0, io.SeekCurrent)
			require.NoError(t, err)
			assert.Equal(t, int64(len(original)), position, "verification must not move the caller's file position")
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			assert.Equal(t, original, data, "verification must not modify the recording")

			require.NoError(t, TruncateIncompleteRecording(file, validBytes))
			data, err = os.ReadFile(path)
			require.NoError(t, err)
			assert.Equal(t, prefix, data)
			validBytes, err = VerifyRecording(file)
			require.NoError(t, err)
			assert.Equal(t, int64(len(prefix)), validBytes)
		})
	}
}

func TestRecordingCheckErrorsDoNotTruncate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recording.pb")
	original := append(recordingTestFile(t, Record{TimestampUnixNano: 1, Payload: []byte("a")}), 0)
	require.NoError(t, os.WriteFile(path, original, 0o600))
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	require.Error(t, TruncateIncompleteRecording(file, -1))
	require.Error(t, TruncateIncompleteRecording(file, int64(len(original)+1)))
	require.NoError(t, file.Close())
	_, err = VerifyRecording(file)
	require.Error(t, err)
	require.NotErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Error(t, TruncateIncompleteRecording(file, 0))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, original, data)
}

func recordingTestFile(t *testing.T, records ...Record) []byte {
	t.Helper()
	var data bytes.Buffer
	require.NoError(t, WriteHeader(&data))
	for _, record := range records {
		require.NoError(t, WriteFrame(&data, record))
	}
	return data.Bytes()
}

func TestRecordingFileHeader(t *testing.T) {
	data := recordingTestFile(t)
	require.Equal(t, "OTELRR\r\n", string(data[:8]))
	require.Equal(t, uint32(len(data)-12), binary.BigEndian.Uint32(data[8:12]))
	var header Header
	require.NoError(t, json.Unmarshal(data[12:], &header), "the file header must be independently readable JSON")
	require.Equal(t, uint16(1), header.Version)
	require.Len(t, header.Fields, 2)
	assert.Equal(t, "timestamp_unix_nano", header.Fields[0].Name)
	assert.Equal(t, "int64 big-endian", header.Fields[0].Encoding)
	assert.Contains(t, header.Fields[0].Description, "exporter write time")
	assert.Equal(t, "payload", header.Fields[1].Name)
	assert.Contains(t, header.Fields[1].Description, "OTLP Protobuf")
	decoded, err := ReadHeader(bytes.NewReader(data))
	require.NoError(t, err)
	assert.Equal(t, header, decoded)
	for size := range len(data) {
		_, err = ReadHeader(bytes.NewReader(data[:size]))
		require.Error(t, err, "header truncated at byte %d", size)
	}
}

func TestRecordingHeaderFailuresNeverRepair(t *testing.T) {
	valid := recordingTestFile(t)
	badVersion := NewHeader()
	badVersion.Version++
	badFields := NewHeader()
	badFields.Fields[0].Encoding = "int64 little-endian"
	badLayout := NewHeader()
	badLayout.Fields = badLayout.Fields[:1]
	badFraming := NewHeader()
	badFraming.Framing = "varint"
	encodeHeader := func(header Header) []byte {
		body, err := json.Marshal(header)
		require.NoError(t, err)
		prefix := append([]byte("OTELRR\r\n"), make([]byte, 4)...)
		binary.BigEndian.PutUint32(prefix[8:], uint32(len(body)))
		return append(prefix, body...)
	}
	for _, tt := range []struct {
		name string
		data []byte
		want string
	}{
		{name: "legacy", data: []byte{0, 0, 0, 4, 'o', 'l', 'd', '!'}, want: "not a versioned RecordReplay"},
		{name: "partial magic", data: valid[:4], want: "invalid recording file header"},
		{name: "partial header length", data: valid[:10], want: "invalid recording file header"},
		{name: "partial header JSON", data: valid[:len(valid)-1], want: "invalid recording file header"},
		{name: "oversized header", data: append([]byte("OTELRR\r\n"), 255, 255, 255, 255), want: "header length"},
		{name: "empty header", data: append([]byte("OTELRR\r\n"), 0, 0, 0, 0), want: "header length"},
		{name: "invalid JSON", data: append([]byte("OTELRR\r\n"), 0, 0, 0, 1, '!'), want: "JSON"},
		{name: "unsupported version", data: encodeHeader(badVersion), want: "unsupported recording version"},
		{name: "unsupported field encoding", data: encodeHeader(badFields), want: "unsupported recording record fields"},
		{name: "unsupported field count", data: encodeHeader(badLayout), want: "unsupported recording record layout"},
		{name: "unsupported framing", data: encodeHeader(badFraming), want: "unsupported recording record layout"},
		{name: "invalid record length", data: append(bytes.Clone(valid), 0, 0, 0, 7), want: "timestamp requires"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "recording.pb")
			require.NoError(t, os.WriteFile(path, tt.data, 0o600))
			file, err := os.OpenFile(path, os.O_RDWR, 0)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, file.Close()) })
			_, err = VerifyRecording(file)
			require.ErrorContains(t, err, tt.want)
			if tt.name == "invalid record length" {
				require.NotErrorIs(t, err, io.ErrUnexpectedEOF, "invalid lengths are not incomplete tails")
			} else {
				require.ErrorIs(t, err, ErrInvalidHeader, "header failures must be distinguished from recoverable tails")
			}
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			assert.Equal(t, tt.data, after)
		})
	}
}

func TestWriteRecordingHeaderErrors(t *testing.T) {
	writeErr := errors.New("header write failed")
	require.ErrorIs(t, WriteHeader(recordingTestWriter(func([]byte) (int, error) {
		return 0, writeErr
	})), writeErr)
	require.ErrorIs(t, WriteHeader(recordingTestWriter(func(data []byte) (int, error) {
		return len(data) - 1, nil
	})), io.ErrShortWrite)
}

func TestWriteRecordingBatchTimestamp(t *testing.T) {
	start := time.Now().UnixNano()
	var data bytes.Buffer
	require.NoError(t, WriteBatch(&data, []byte("batch")))
	end := time.Now().UnixNano()
	record, err := ReadFrame(&data, 5)
	require.NoError(t, err)
	assert.Equal(t, []byte("batch"), record.Payload)
	assert.GreaterOrEqual(t, record.TimestampUnixNano, start)
	assert.LessOrEqual(t, record.TimestampUnixNano, end)
}
