// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/config/configtls"
)

type Config struct {
	File              string                 `mapstructure:"file"`
	Signal            string                 `mapstructure:"signal"`
	Endpoint          string                 `mapstructure:"endpoint"`
	Protocol          string                 `mapstructure:"protocol"`
	TLS               configtls.ClientConfig `mapstructure:"tls"`
	LatenessTolerance time.Duration          `mapstructure:"lateness_tolerance"`
	Timeout           time.Duration          `mapstructure:"timeout"`
	MaxPayloadSize    uint32                 `mapstructure:"max_payload_size"`
	RebaseTimestamps  bool                   `mapstructure:"rebase_timestamps"`
}

func DefaultConfig() Config {
	return Config{
		Protocol:          "grpc",
		TLS:               configtls.NewDefaultClientConfig(),
		LatenessTolerance: 10 * time.Millisecond,
		Timeout:           10 * time.Second,
		MaxPayloadSize:    64 * 1024 * 1024,
	}
}

func (c Config) Validate() error {
	if c.File == "" {
		return errors.New("replay requires a recording file as an argument or replay.file")
	}
	switch c.Signal {
	case "logs", "metrics", "traces":
	default:
		return errors.New("replay.signal must be logs, metrics, or traces; recordings do not identify their signal")
	}
	if c.Endpoint == "" {
		return errors.New("replay.endpoint is required")
	}
	switch c.Protocol {
	case "grpc":
		host, port, err := net.SplitHostPort(c.Endpoint)
		if err != nil {
			return fmt.Errorf("replay.endpoint must be host:port for grpc: %w", err)
		}
		number, err := strconv.Atoi(port)
		if host == "" || err != nil || number < 1 || number > 65535 {
			return errors.New("replay.endpoint requires a host and numeric port between 1 and 65535")
		}
	case "http":
		u, err := url.Parse(c.Endpoint)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return errors.New("replay.endpoint must be an http(s) base URL without credentials, query, or fragment")
		}
		if u.Scheme == "http" && !c.TLS.Insecure {
			return errors.New("HTTP without TLS requires replay.tls.insecure: true")
		}
		if u.Scheme == "https" && c.TLS.Insecure {
			return errors.New("HTTPS requires replay.tls.insecure: false")
		}
	default:
		return errors.New("replay.protocol must be grpc or http (OTLP/HTTP Protobuf)")
	}
	if c.LatenessTolerance < 0 {
		return errors.New("replay.lateness_tolerance must not be negative")
	}
	if c.Timeout <= 0 {
		return errors.New("replay.timeout must be positive")
	}
	if c.MaxPayloadSize == 0 || c.MaxPayloadSize > math.MaxUint32-TimestampSize {
		return errors.New("replay.max_payload_size must be between 1 and uint32_max - 8 bytes")
	}
	return nil
}
