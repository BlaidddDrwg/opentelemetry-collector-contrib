// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type Sender struct {
	Prepare PrepareFunc
	Close   func() error
}

func NewSender(ctx context.Context, cfg Config, prepareConnection bool) (Sender, error) {
	if err := ctx.Err(); err != nil {
		return Sender{}, err
	}
	tlsConfig, err := cfg.TLS.LoadTLSConfig(ctx)
	if err != nil {
		return Sender{}, err
	}
	if cfg.Protocol == "http" {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = tlsConfig
		client := &http.Client{
			Transport: transport,
			Timeout:   cfg.Timeout,
			// A redirect changes the intended destination and timing.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
		endpoint, joinErr := url.JoinPath(cfg.Endpoint, "v1", cfg.Signal)
		if joinErr != nil {
			return Sender{}, joinErr
		}
		var connection net.Conn
		var connectionMu sync.Mutex
		if prepareConnection {
			u, _ := url.Parse(endpoint)
			port := u.Port()
			if port == "" {
				port = "80"
				if u.Scheme == "https" {
					port = "443"
				}
			}
			address := net.JoinHostPort(u.Hostname(), port)
			readyCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
			defer cancel()
			dialer := &net.Dialer{}
			if u.Scheme == "https" {
				connection, err = (&tls.Dialer{NetDialer: dialer, Config: tlsConfig}).DialContext(readyCtx, "tcp", address)
			} else {
				connection, err = dialer.DialContext(readyCtx, "tcp", address)
			}
			if err != nil {
				return Sender{}, fmt.Errorf("cannot establish replay HTTP connection: %w", err)
			}
			// Hand the prepared connection to the first export without sending a
			// probe request or bypassing the normal HTTP connection pool.
			takeConnection := func(ctx context.Context, network, address string) (net.Conn, error) {
				connectionMu.Lock()
				conn := connection
				connection = nil
				connectionMu.Unlock()
				if conn != nil {
					return conn, nil
				}
				if u.Scheme == "https" {
					return (&tls.Dialer{NetDialer: dialer, Config: tlsConfig}).DialContext(ctx, network, address)
				}
				return dialer.DialContext(ctx, network, address)
			}
			transport.Proxy = nil
			if u.Scheme == "https" {
				transport.DialTLSContext = takeConnection
			} else {
				transport.DialContext = takeConnection
			}
		}
		return Sender{
			Prepare: func(payload []byte, shift time.Duration) (func(context.Context) error, error) {
				data, prepareErr := prepareReplayHTTPPayload(cfg.Signal, payload, shift)
				if prepareErr != nil {
					return nil, prepareErr
				}
				return func(ctx context.Context) error {
					return sendReplayHTTP(ctx, client, endpoint, cfg.Signal, data)
				}, nil
			},
			Close: func() error {
				transport.CloseIdleConnections()
				connectionMu.Lock()
				defer connectionMu.Unlock()
				if connection != nil {
					return connection.Close()
				}
				return nil
			},
		}, nil
	}
	var creds credentials.TransportCredentials
	if cfg.TLS.Insecure {
		creds = insecure.NewCredentials()
	} else {
		creds = credentials.NewTLS(tlsConfig)
	}
	conn, err := grpc.NewClient(cfg.Endpoint,
		grpc.WithTransportCredentials(creds),
		grpc.WithDisableRetry(),
		grpc.WithDisableServiceConfig(),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(int(cfg.MaxPayloadSize))),
	)
	if err != nil {
		return Sender{}, err
	}
	readyCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	conn.Connect()
	for state := conn.GetState(); state != connectivity.Ready; state = conn.GetState() {
		if !conn.WaitForStateChange(readyCtx, state) {
			return Sender{}, errors.Join(fmt.Errorf("cannot connect to replay endpoint: %w", readyCtx.Err()), conn.Close())
		}
	}
	return Sender{Prepare: grpcReplayPreparer(conn, cfg.Signal), Close: conn.Close}, nil
}

func grpcReplayPreparer(conn *grpc.ClientConn, signal string) PrepareFunc {
	return func(payload []byte, shift time.Duration) (func(context.Context) error, error) {
		switch signal {
		case "logs":
			req := plogotlp.NewExportRequest()
			if err := req.UnmarshalProto(payload); err != nil {
				return nil, err
			}
			if err := rebaseReplayLogs(req.Logs(), shift); err != nil {
				return nil, err
			}
			return func(ctx context.Context) error {
				response, err := plogotlp.NewGRPCClient(conn).Export(ctx, req)
				if err != nil {
					return err
				}
				partial := response.PartialSuccess()
				return checkReplayPartial(partial.RejectedLogRecords(), partial.ErrorMessage())
			}, nil
		case "metrics":
			req := pmetricotlp.NewExportRequest()
			if err := req.UnmarshalProto(payload); err != nil {
				return nil, err
			}
			if err := rebaseReplayMetrics(req.Metrics(), shift); err != nil {
				return nil, err
			}
			return func(ctx context.Context) error {
				response, err := pmetricotlp.NewGRPCClient(conn).Export(ctx, req)
				if err != nil {
					return err
				}
				partial := response.PartialSuccess()
				return checkReplayPartial(partial.RejectedDataPoints(), partial.ErrorMessage())
			}, nil
		default:
			req := ptraceotlp.NewExportRequest()
			if err := req.UnmarshalProto(payload); err != nil {
				return nil, err
			}
			if err := rebaseReplayTraces(req.Traces(), shift); err != nil {
				return nil, err
			}
			return func(ctx context.Context) error {
				response, err := ptraceotlp.NewGRPCClient(conn).Export(ctx, req)
				if err != nil {
					return err
				}
				partial := response.PartialSuccess()
				return checkReplayPartial(partial.RejectedSpans(), partial.ErrorMessage())
			}, nil
		}
	}
}

func prepareReplayHTTPPayload(signal string, payload []byte, shift time.Duration) ([]byte, error) {
	switch signal {
	case "logs":
		req := plogotlp.NewExportRequest()
		if err := req.UnmarshalProto(payload); err != nil {
			return nil, err
		}
		if shift == 0 {
			return payload, nil
		}
		if err := rebaseReplayLogs(req.Logs(), shift); err != nil {
			return nil, err
		}
		return req.MarshalProto()
	case "metrics":
		req := pmetricotlp.NewExportRequest()
		if err := req.UnmarshalProto(payload); err != nil {
			return nil, err
		}
		if shift == 0 {
			return payload, nil
		}
		if err := rebaseReplayMetrics(req.Metrics(), shift); err != nil {
			return nil, err
		}
		return req.MarshalProto()
	default:
		req := ptraceotlp.NewExportRequest()
		if err := req.UnmarshalProto(payload); err != nil {
			return nil, err
		}
		if shift == 0 {
			return payload, nil
		}
		if err := rebaseReplayTraces(req.Traces(), shift); err != nil {
			return nil, err
		}
		return req.MarshalProto()
	}
}

func sendReplayHTTP(ctx context.Context, client *http.Client, endpoint, signal string, payload []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("OTLP/HTTP export returned %s (not retried)", resp.Status)
	}
	contentType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || contentType != "application/x-protobuf" {
		return errors.New("OTLP/HTTP response must have Content-Type application/x-protobuf")
	}
	const maxResponseSize = 1024 * 1024
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize+1))
	if err != nil {
		return err
	}
	if len(data) > maxResponseSize {
		return errors.New("OTLP/HTTP response exceeds 1 MiB")
	}
	switch signal {
	case "logs":
		response := plogotlp.NewExportResponse()
		if err := response.UnmarshalProto(data); err != nil {
			return err
		}
		partial := response.PartialSuccess()
		return checkReplayPartial(partial.RejectedLogRecords(), partial.ErrorMessage())
	case "metrics":
		response := pmetricotlp.NewExportResponse()
		if err := response.UnmarshalProto(data); err != nil {
			return err
		}
		partial := response.PartialSuccess()
		return checkReplayPartial(partial.RejectedDataPoints(), partial.ErrorMessage())
	default:
		response := ptraceotlp.NewExportResponse()
		if err := response.UnmarshalProto(data); err != nil {
			return err
		}
		partial := response.PartialSuccess()
		return checkReplayPartial(partial.RejectedSpans(), partial.ErrorMessage())
	}
}

func checkReplayPartial(rejected int64, message string) error {
	if rejected != 0 || message != "" {
		return &PartialError{Rejected: rejected, Message: message}
	}
	return nil
}

type PartialError struct {
	Rejected int64
	Message  string
}

func (e *PartialError) Error() string {
	return fmt.Sprintf("OTLP partial-success response: rejected=%d message=%q (not retried)", e.Rejected, e.Message)
}
