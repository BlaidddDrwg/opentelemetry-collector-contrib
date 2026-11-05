// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	replay "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/recordreplay"
)

func recordingTestFile(t *testing.T, records ...replay.Record) []byte {
	t.Helper()
	var data bytes.Buffer
	require.NoError(t, replay.WriteHeader(&data))
	for _, record := range records {
		require.NoError(t, replay.WriteFrame(&data, record))
	}
	return data.Bytes()
}
