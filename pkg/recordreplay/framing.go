// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"
)

// RecordReplay v1 files contain:
//
//	["OTELRR\r\n"][uint32 BE JSON-header length][JSON file header]
//	[uint32 BE record length][int64 BE timestamp Unix nanoseconds][OTLP payload]
//	...additional records...
//
// Lengths exclude their four-byte prefix. The file header versions the record
// layout and describes its fields. Timestamp is exporter write time, not ingress
// arrival time or telemetry event time; wall-clock adjustments can affect it.
// Payload is a reserialized OTLP Protobuf batch, not original transport bytes.
// Signal type remains implicit in the configured file. No checksum is stored.
const (
	Magic         = "OTELRR\r\n"
	Version       = 1
	LengthSize    = 4
	TimestampSize = 8
	MaxHeaderSize = 64 * 1024
)

var ErrInvalidHeader = errors.New("invalid recording file header")

type Record struct {
	TimestampUnixNano int64
	Payload           []byte
}

type Header struct {
	Version uint16  `json:"version"`
	Framing string  `json:"framing"`
	Fields  []Field `json:"fields"`
}

type Field struct {
	Name        string `json:"name"`
	Encoding    string `json:"encoding"`
	Description string `json:"description"`
}

func NewHeader() Header {
	return Header{
		Version: Version,
		Framing: "uint32 big-endian record byte length, excluding the length prefix",
		Fields: []Field{
			{
				Name:        "timestamp_unix_nano",
				Encoding:    "int64 big-endian",
				Description: "UTC Unix nanoseconds captured at exporter write time; not ingress or event time",
			},
			{
				Name:        "payload",
				Encoding:    "remaining record bytes",
				Description: "OTLP Protobuf telemetry batch; signal type is determined by the file configuration",
			},
		},
	}
}

func WriteHeader(writer io.Writer) error {
	header, err := json.Marshal(NewHeader())
	if err != nil {
		return err
	}
	data := append([]byte(Magic), make([]byte, LengthSize)...)
	binary.BigEndian.PutUint32(data[len(Magic):], uint32(len(header)))
	return writeRecordingBytes(writer, append(data, header...))
}

// ReadHeader must precede frame reading. Unknown versions or layouts
// are rejected rather than guessed. The header length is bounded before allocation.
func ReadHeader(reader io.Reader) (Header, error) {
	var magic [len(Magic)]byte
	if _, err := io.ReadFull(reader, magic[:]); err != nil {
		return Header{}, err
	}
	if string(magic[:]) != Magic {
		return Header{}, errors.New("not a versioned RecordReplay file; use a fresh path or confirmed --overwrite")
	}
	length, err := readRecordingUint32(reader)
	if err != nil {
		return Header{}, err
	}
	if length == 0 || length > MaxHeaderSize {
		return Header{}, fmt.Errorf("invalid recording file header length %d", length)
	}
	data := make([]byte, length)
	if _, err = io.ReadFull(reader, data); err != nil {
		return Header{}, err
	}
	var header Header
	if err = json.Unmarshal(data, &header); err != nil {
		return Header{}, fmt.Errorf("invalid recording file header JSON: %w", err)
	}
	if header.Version != Version {
		return Header{}, fmt.Errorf("unsupported recording version %d; supported version is %d", header.Version, Version)
	}
	expected := NewHeader()
	if header.Framing != expected.Framing || len(header.Fields) != len(expected.Fields) {
		return Header{}, errors.New("unsupported recording record layout")
	}
	for i, field := range header.Fields {
		if field.Name != expected.Fields[i].Name || field.Encoding != expected.Fields[i].Encoding {
			return Header{}, errors.New("unsupported recording record fields")
		}
	}
	return header, nil
}

func WriteBatch(writer io.Writer, payload []byte) error {
	return WriteFrame(writer, Record{
		TimestampUnixNano: time.Now().UnixNano(),
		Payload:           payload,
	})
}

// WriteFrame writes one complete frame in a single Write call.
// The caller must serialize concurrent writes and arrange flushing/syncing.
// A single Write does not guarantee crash atomicity.
func WriteFrame(writer io.Writer, record Record) error {
	if uint64(len(record.Payload)) > math.MaxUint32-TimestampSize {
		return errors.New("recording payload exceeds the uint32 frame length")
	}
	frame := make([]byte, LengthSize+TimestampSize, LengthSize+TimestampSize+len(record.Payload))
	binary.BigEndian.PutUint32(frame, uint32(TimestampSize+len(record.Payload)))
	binary.BigEndian.PutUint64(frame[LengthSize:], uint64(record.TimestampUnixNano))
	return writeRecordingBytes(writer, append(frame, record.Payload...))
}

func writeRecordingBytes(writer io.Writer, data []byte) error {
	n, err := writer.Write(data)
	if err == nil && n != len(data) {
		return io.ErrShortWrite
	}
	return err
}

// ReadFrame reads a record after the file header, returning io.EOF only
// at a frame boundary and io.ErrUnexpectedEOF for an incomplete frame.
// The caller supplies an allocation limit; exceeding it consumes the length
// prefix but not the timestamp or payload. Errors end reading until repositioned.
func ReadFrame(reader io.Reader, maxPayloadSize uint32) (Record, error) {
	length, err := readRecordingLength(reader)
	if err != nil {
		return Record{}, err
	}
	payloadLength := length - TimestampSize
	if payloadLength > maxPayloadSize {
		return Record{}, fmt.Errorf("recording payload length %d exceeds limit %d", payloadLength, maxPayloadSize)
	}
	var timestamp [TimestampSize]byte
	if err := readRecordingPart(reader, timestamp[:]); err != nil {
		return Record{}, err
	}
	payload := make([]byte, payloadLength)
	if err := readRecordingPart(reader, payload); err != nil {
		return Record{}, err
	}
	return Record{TimestampUnixNano: int64(binary.BigEndian.Uint64(timestamp[:])), Payload: payload}, nil
}

func readRecordingPart(reader io.Reader, data []byte) error {
	if _, err := io.ReadFull(reader, data); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	return nil
}

func readRecordingLength(reader io.Reader) (uint32, error) {
	length, err := readRecordingUint32(reader)
	if err == nil && length < TimestampSize {
		return 0, fmt.Errorf("invalid recording frame length %d: timestamp requires %d bytes", length, TimestampSize)
	}
	return length, err
}

func readRecordingUint32(reader io.Reader) (uint32, error) {
	var header [LengthSize]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(header[:]), nil
}

// VerifyRecording returns the end of the last complete length-prefixed frame.
// Incomplete frames return io.ErrUnexpectedEOF. Header failures additionally wrap
// ErrInvalidHeader and must never trigger truncation, even if their cause
// is io.ErrUnexpectedEOF. An empty file can be initialized before append.
// Payloads are skipped without allocating or deserializing them.
func VerifyRecording(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	if !info.Mode().IsRegular() {
		return 0, errors.New("recording must be a regular file")
	}
	if info.Size() == 0 {
		return 0, nil
	}

	reader := io.NewSectionReader(file, 0, info.Size())
	if _, err = ReadHeader(reader); err != nil {
		return 0, fmt.Errorf("%w (cannot repair by truncating a record): %w", ErrInvalidHeader, err)
	}
	offset, err := reader.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	for offset < info.Size() {
		length, readErr := readRecordingLength(reader)
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				readErr = io.ErrUnexpectedEOF
			}
			return offset, readErr
		}
		if int64(length) > info.Size()-offset-LengthSize {
			return offset, io.ErrUnexpectedEOF
		}
		if _, err = reader.Seek(int64(length), io.SeekCurrent); err != nil {
			return offset, err
		}
		offset += LengthSize + int64(length)
	}
	return offset, nil
}

// TruncateIncompleteRecording removes only bytes after the verified boundary.
func TruncateIncompleteRecording(file *os.File, validBytes int64) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if validBytes < 0 || validBytes > info.Size() {
		return errors.New("invalid recording truncation boundary")
	}
	if validBytes == info.Size() {
		return nil
	}
	if err := file.Truncate(validBytes); err != nil {
		return err
	}
	return file.Sync()
}
