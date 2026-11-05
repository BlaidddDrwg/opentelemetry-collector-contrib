// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestReplayTimestampFields(t *testing.T) {
	logs := plog.NewLogs()
	records := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	record := records.AppendEmpty()
	record.SetTimestamp(100)
	record.SetObservedTimestamp(110)
	record.Body().SetStr("unchanged")
	unset := records.AppendEmpty()
	require.NoError(t, rebaseReplayLogs(logs, 1000))
	assert.Equal(t, pcommon.Timestamp(1100), record.Timestamp())
	assert.Equal(t, pcommon.Timestamp(1110), record.ObservedTimestamp())
	assert.Equal(t, "unchanged", record.Body().Str())
	assert.Zero(t, unset.Timestamp())
	assert.Zero(t, unset.ObservedTimestamp())

	traces := ptrace.NewTraces()
	span := traces.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("unchanged")
	span.SetStartTimestamp(100)
	span.SetEndTimestamp(130)
	event := span.Events().AppendEmpty()
	event.SetTimestamp(115)
	require.NoError(t, rebaseReplayTraces(traces, 1000))
	assert.Equal(t, pcommon.Timestamp(1100), span.StartTimestamp())
	assert.Equal(t, pcommon.Timestamp(1130), span.EndTimestamp())
	assert.Equal(t, pcommon.Timestamp(1115), event.Timestamp())
	assert.Equal(t, "unchanged", span.Name())

	for _, metricType := range []pmetric.MetricType{
		pmetric.MetricTypeGauge, pmetric.MetricTypeSum, pmetric.MetricTypeHistogram,
		pmetric.MetricTypeExponentialHistogram, pmetric.MetricTypeSummary,
	} {
		t.Run(metricType.String(), func(t *testing.T) {
			metrics := pmetric.NewMetrics()
			metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			metric.SetName("unchanged")
			var point replayMetricPoint
			var exemplars pmetric.ExemplarSlice
			switch metricType {
			case pmetric.MetricTypeGauge:
				number := metric.SetEmptyGauge().DataPoints().AppendEmpty()
				number.SetIntValue(42)
				point, exemplars = number, number.Exemplars()
			case pmetric.MetricTypeSum:
				number := metric.SetEmptySum().DataPoints().AppendEmpty()
				point, exemplars = number, number.Exemplars()
			case pmetric.MetricTypeHistogram:
				histogram := metric.SetEmptyHistogram().DataPoints().AppendEmpty()
				point, exemplars = histogram, histogram.Exemplars()
			case pmetric.MetricTypeExponentialHistogram:
				histogram := metric.SetEmptyExponentialHistogram().DataPoints().AppendEmpty()
				point, exemplars = histogram, histogram.Exemplars()
			case pmetric.MetricTypeSummary:
				point = metric.SetEmptySummary().DataPoints().AppendEmpty()
			case pmetric.MetricTypeEmpty:
				t.Fatal("test requires a data point")
			}
			point.SetStartTimestamp(50)
			point.SetTimestamp(100)
			if metricType != pmetric.MetricTypeSummary {
				exemplars.AppendEmpty().SetTimestamp(75)
			}
			require.NoError(t, rebaseReplayMetrics(metrics, 1000))
			assert.Equal(t, pcommon.Timestamp(1050), point.StartTimestamp())
			assert.Equal(t, pcommon.Timestamp(1100), point.Timestamp())
			if metricType != pmetric.MetricTypeSummary {
				assert.Equal(t, pcommon.Timestamp(1075), exemplars.At(0).Timestamp())
			}
			assert.Equal(t, "unchanged", metric.Name())
			if metricType == pmetric.MetricTypeGauge {
				assert.Equal(t, int64(42), metric.Gauge().DataPoints().At(0).IntValue())
			}
		})
	}
}

func TestReplayTimestampBounds(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value pcommon.Timestamp
		shift time.Duration
		want  pcommon.Timestamp
		error bool
	}{
		{name: "positive", value: 100, shift: 50, want: 150},
		{name: "negative", value: 100, shift: -50, want: 50},
		{name: "unset stays unset", shift: 50},
		{name: "zero shift", value: 100, want: 100},
		{name: "overflow", value: math.MaxUint64, shift: 1, error: true},
		{name: "underflow", value: 10, shift: -11, error: true},
		{name: "would become unset", value: 10, shift: -10, error: true},
		{name: "minimum duration", value: 1 << 63, shift: time.Duration(math.MinInt64), error: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			value := tt.value
			err := rebaseReplayTimestamp(value, tt.shift, func(timestamp pcommon.Timestamp) { value = timestamp })
			if tt.error {
				require.Error(t, err)
				assert.Equal(t, tt.value, value)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, value)
			}
		})
	}
	_, err := TimestampShift(math.MinInt64, 0)
	require.ErrorContains(t, err, "exceeds time.Duration range")
	_, err = TimestampShift(math.MaxInt64, -2)
	require.ErrorContains(t, err, "exceeds time.Duration range")
	shift, err := TimestampShift(200, 100)
	require.NoError(t, err)
	assert.Equal(t, -100*time.Nanosecond, shift)
}
