// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

type Inspection struct {
	SizeBytes              int64  `json:"sizeBytes"`
	Batches                uint64 `json:"batches"`
	Items                  uint64 `json:"items"`
	FirstTimestampUnixNano int64  `json:"firstTimestampUnixNano"`
	LastTimestampUnixNano  int64  `json:"lastTimestampUnixNano"`
	DurationNanos          int64  `json:"durationNanos"`
}

func OpenInput(path string) (*os.File, int64, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, 0, fmt.Errorf("cannot stat recording input: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, 0, errors.New("recording input must be an accessible regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("cannot open recording input: %w", err)
	}
	actual, err := file.Stat()
	if err != nil || !actual.Mode().IsRegular() || !os.SameFile(info, actual) {
		_ = file.Close()
		return nil, 0, errors.New("recording input changed while opening")
	}
	return file, actual.Size(), nil
}

func NewReader(file *os.File, size int64) (*bufio.Reader, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || size < 0 || size > info.Size() {
		return nil, errors.New("recording snapshot requires a regular file and a valid bounded size")
	}
	reader := bufio.NewReader(io.NewSectionReader(file, 0, size))
	if size == 0 {
		return reader, nil
	}
	if _, err := ReadHeader(reader); err != nil {
		return nil, fmt.Errorf("cannot read recording header: %w", err)
	}
	return reader, nil
}

func Inspect(ctx context.Context, file *os.File, size int64, signal string, limit uint32) (Inspection, error) {
	result := Inspection{SizeBytes: size}
	if signal != "logs" && signal != "metrics" && signal != "traces" {
		return result, errors.New("signal must be logs, metrics, or traces")
	}
	reader, err := NewReader(file, size)
	if err != nil {
		return result, err
	}
	for {
		if contextErr := ctx.Err(); contextErr != nil {
			return result, contextErr
		}
		record, readErr := ReadFrame(reader, limit)
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return result, fmt.Errorf("invalid, oversized, or incomplete recording frame: %w", readErr)
		}
		items, payloadErr := PayloadItems(signal, record.Payload)
		if payloadErr != nil {
			return result, payloadErr
		}
		if result.Batches == 0 || record.TimestampUnixNano < result.FirstTimestampUnixNano {
			result.FirstTimestampUnixNano = record.TimestampUnixNano
		}
		if result.Batches == 0 || record.TimestampUnixNano > result.LastTimestampUnixNano {
			result.LastTimestampUnixNano = record.TimestampUnixNano
		}
		if math.MaxUint64-result.Items < items {
			return result, errors.New("recording item count overflows uint64")
		}
		result.Batches++
		result.Items += items
	}
	duration, err := TimestampShift(result.FirstTimestampUnixNano, result.LastTimestampUnixNano)
	if err != nil {
		return result, errors.New("recording duration exceeds int64 nanoseconds")
	}
	result.DurationNanos = int64(duration)
	return result, nil
}

func PayloadItems(signal string, payload []byte) (uint64, error) {
	var count int
	var err error
	switch signal {
	case "logs":
		req := plogotlp.NewExportRequest()
		err = req.UnmarshalProto(payload)
		if err == nil {
			count = req.Logs().LogRecordCount()
		}
	case "metrics":
		req := pmetricotlp.NewExportRequest()
		err = req.UnmarshalProto(payload)
		if err == nil {
			count = req.Metrics().DataPointCount()
		}
	case "traces":
		req := ptraceotlp.NewExportRequest()
		err = req.UnmarshalProto(payload)
		if err == nil {
			count = req.Traces().SpanCount()
		}
	default:
		return 0, errors.New("invalid recording signal")
	}
	if err != nil {
		return 0, errors.New("invalid OTLP payload for selected signal")
	}
	return uint64(count), nil
}
