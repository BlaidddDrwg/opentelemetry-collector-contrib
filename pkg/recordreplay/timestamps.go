// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"errors"
	"math"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TimestampShift(recorded, current int64) (time.Duration, error) {
	if (recorded < 0 && current > math.MaxInt64+recorded) || (recorded > 0 && current < math.MinInt64+recorded) {
		return 0, errors.New("timestamp rebase offset exceeds time.Duration range")
	}
	return time.Duration(current - recorded), nil
}

func rebaseReplayTimestamp(timestamp pcommon.Timestamp, shift time.Duration, set func(pcommon.Timestamp)) error {
	if timestamp == 0 || shift == 0 {
		return nil
	}
	value := uint64(timestamp)
	if shift > 0 {
		if value > math.MaxUint64-uint64(shift) {
			return errors.New("rebased payload timestamp exceeds uint64 range")
		}
		value += uint64(shift)
	} else {
		delta := uint64(-(shift + 1)) + 1
		if value <= delta {
			return errors.New("rebased payload timestamp would be negative or unset")
		}
		value -= delta
	}
	set(pcommon.Timestamp(value))
	return nil
}

func rebaseReplayLogs(logs plog.Logs, shift time.Duration) error {
	if shift == 0 {
		return nil
	}
	for _, resource := range logs.ResourceLogs().All() {
		for _, scope := range resource.ScopeLogs().All() {
			for _, record := range scope.LogRecords().All() {
				if err := rebaseReplayTimestamp(record.Timestamp(), shift, record.SetTimestamp); err != nil {
					return err
				}
				if err := rebaseReplayTimestamp(record.ObservedTimestamp(), shift, record.SetObservedTimestamp); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func rebaseReplayTraces(traces ptrace.Traces, shift time.Duration) error {
	if shift == 0 {
		return nil
	}
	for _, resource := range traces.ResourceSpans().All() {
		for _, scope := range resource.ScopeSpans().All() {
			for _, span := range scope.Spans().All() {
				if err := rebaseReplayTimestamp(span.StartTimestamp(), shift, span.SetStartTimestamp); err != nil {
					return err
				}
				if err := rebaseReplayTimestamp(span.EndTimestamp(), shift, span.SetEndTimestamp); err != nil {
					return err
				}
				for _, event := range span.Events().All() {
					if err := rebaseReplayTimestamp(event.Timestamp(), shift, event.SetTimestamp); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

type replayMetricPoint interface {
	Timestamp() pcommon.Timestamp
	SetTimestamp(pcommon.Timestamp)
	StartTimestamp() pcommon.Timestamp
	SetStartTimestamp(pcommon.Timestamp)
}

func rebaseReplayMetricPoint(point replayMetricPoint, shift time.Duration) error {
	if err := rebaseReplayTimestamp(point.StartTimestamp(), shift, point.SetStartTimestamp); err != nil {
		return err
	}
	return rebaseReplayTimestamp(point.Timestamp(), shift, point.SetTimestamp)
}

func rebaseReplayExemplars(exemplars pmetric.ExemplarSlice, shift time.Duration) error {
	for _, exemplar := range exemplars.All() {
		if err := rebaseReplayTimestamp(exemplar.Timestamp(), shift, exemplar.SetTimestamp); err != nil {
			return err
		}
	}
	return nil
}

func rebaseReplayMetrics(metrics pmetric.Metrics, shift time.Duration) error {
	if shift == 0 {
		return nil
	}
	for _, resource := range metrics.ResourceMetrics().All() {
		for _, scope := range resource.ScopeMetrics().All() {
			for _, metric := range scope.Metrics().All() {
				if err := rebaseReplayMetric(metric, shift); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func rebaseReplayMetric(metric pmetric.Metric, shift time.Duration) error {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		return rebaseReplayNumberPoints(metric.Gauge().DataPoints(), shift)
	case pmetric.MetricTypeSum:
		return rebaseReplayNumberPoints(metric.Sum().DataPoints(), shift)
	case pmetric.MetricTypeHistogram:
		for _, point := range metric.Histogram().DataPoints().All() {
			if err := rebaseReplayMetricPoint(point, shift); err != nil {
				return err
			}
			if err := rebaseReplayExemplars(point.Exemplars(), shift); err != nil {
				return err
			}
		}
	case pmetric.MetricTypeExponentialHistogram:
		for _, point := range metric.ExponentialHistogram().DataPoints().All() {
			if err := rebaseReplayMetricPoint(point, shift); err != nil {
				return err
			}
			if err := rebaseReplayExemplars(point.Exemplars(), shift); err != nil {
				return err
			}
		}
	case pmetric.MetricTypeSummary:
		for _, point := range metric.Summary().DataPoints().All() {
			if err := rebaseReplayMetricPoint(point, shift); err != nil {
				return err
			}
		}
	case pmetric.MetricTypeEmpty:
	}
	return nil
}

func rebaseReplayNumberPoints(points pmetric.NumberDataPointSlice, shift time.Duration) error {
	for _, point := range points.All() {
		if err := rebaseReplayMetricPoint(point, shift); err != nil {
			return err
		}
		if err := rebaseReplayExemplars(point.Exemplars(), shift); err != nil {
			return err
		}
	}
	return nil
}
