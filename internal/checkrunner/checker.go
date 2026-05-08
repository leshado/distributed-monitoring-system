package checkrunner

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"distributed-monitoring-system/internal/domain"

	"github.com/tidwall/gjson"
	"go.opentelemetry.io/otel"
)

const maxBodyBytes = 1 << 20

type Result struct {
	Status     domain.CheckStatus
	Latency    time.Duration
	StatusCode int
	Error      string
}

type Checker interface {
	Run(ctx context.Context, check domain.Check) Result
}

type DefaultChecker struct {
	httpClient *http.Client
}

func New() *DefaultChecker {
	return &DefaultChecker{
		httpClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        512,
				MaxIdleConnsPerHost: 128,
				IdleConnTimeout:     90 * time.Second,
				TLSHandshakeTimeout: 5 * time.Second,
			},
		},
	}
}

func (c *DefaultChecker) Run(ctx context.Context, check domain.Check) Result {
	ctx, span := otel.Tracer("check-runner").Start(ctx, "checker.run")
	defer span.End()

	timeout := check.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	var statusCode int
	err := c.runByType(ctx, check, &statusCode)
	latency := time.Since(start)
	if err != nil {
		return Result{Status: domain.CheckStatusFail, Latency: latency, StatusCode: statusCode, Error: err.Error()}
	}
	return Result{Status: domain.CheckStatusOK, Latency: latency, StatusCode: statusCode}
}

func (c *DefaultChecker) runByType(ctx context.Context, check domain.Check, statusCode *int) error {
	switch check.Type {
	case domain.CheckTypeHTTP:
		return c.runHTTP(ctx, check, statusCode)
	case domain.CheckTypeTCP:
		return runTCP(ctx, check)
	case domain.CheckTypeTLS:
		return runTLS(ctx, check)
	default:
		return fmt.Errorf("unsupported check type %q", check.Type)
	}
}

func (c *DefaultChecker) runHTTP(ctx context.Context, check domain.Check, statusCode *int) error {
	cfg := check.Config.HTTP
	if cfg == nil {
		return errors.New("missing http check config")
	}
	method := strings.TrimSpace(cfg.Method)
	if method == "" {
		method = http.MethodGet
	}
	req, err := http.NewRequestWithContext(ctx, method, cfg.URL, nil)
	if err != nil {
		return fmt.Errorf("build http request: %w", err)
	}
	for k, v := range cfg.Headers {
		req.Header.Set(k, v)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	*statusCode = resp.StatusCode
	expected := cfg.ExpectedCode
	if expected == 0 {
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("unexpected status %d", resp.StatusCode)
		}
	} else if resp.StatusCode != expected {
		return fmt.Errorf("unexpected status %d, expected %d", resp.StatusCode, expected)
	}

	if cfg.BodyContains == "" && cfg.JSONPath == "" {
		return nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}
	if cfg.BodyContains != "" && !strings.Contains(string(body), cfg.BodyContains) {
		return fmt.Errorf("body does not contain %q", cfg.BodyContains)
	}
	if cfg.JSONPath != "" {
		value, err := jsonPathValue(body, cfg.JSONPath)
		if err != nil {
			return err
		}
		if value != cfg.JSONEquals {
			return fmt.Errorf("json path %q value %q does not equal %q", cfg.JSONPath, value, cfg.JSONEquals)
		}
	}
	return nil
}

func runTCP(ctx context.Context, check domain.Check) error {
	cfg := check.Config.TCP
	if cfg == nil {
		return errors.New("missing tcp check config")
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", cfg.Address)
	if err != nil {
		return fmt.Errorf("tcp dial failed: %w", err)
	}
	return conn.Close()
}

func runTLS(ctx context.Context, check domain.Check) error {
	cfg := check.Config.TLS
	if cfg == nil {
		return errors.New("missing tls check config")
	}
	serverName := cfg.ServerName
	if serverName == "" {
		host, _, err := net.SplitHostPort(cfg.Address)
		if err != nil {
			return fmt.Errorf("parse tls address: %w", err)
		}
		serverName = host
	}
	conn, err := (&tls.Dialer{
		NetDialer: &net.Dialer{},
		Config: &tls.Config{
			ServerName:         serverName,
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: cfg.InsecureSkipVerify,
		},
	}).DialContext(ctx, "tcp", cfg.Address)
	if err != nil {
		return fmt.Errorf("tls dial failed: %w", err)
	}
	defer func() { _ = conn.Close() }()
	tlsConn, ok := conn.(*tls.Conn)
	if !ok {
		return errors.New("connection is not tls")
	}
	state := tlsConn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return errors.New("server did not provide a certificate")
	}
	minDays := cfg.MinDaysValidity
	if minDays <= 0 {
		minDays = 14
	}
	if time.Until(state.PeerCertificates[0].NotAfter) < time.Duration(minDays)*24*time.Hour {
		return fmt.Errorf("certificate expires at %s", state.PeerCertificates[0].NotAfter.Format(time.RFC3339))
	}
	return nil
}

func jsonPathValue(body []byte, path string) (string, error) {
	normalized := strings.TrimSpace(path)
	normalized = strings.TrimPrefix(normalized, "$.")
	normalized = strings.TrimPrefix(normalized, "$")
	normalized = strings.TrimPrefix(normalized, ".")

	result := gjson.GetBytes(body, normalized)
	if !result.Exists() {
		return "", fmt.Errorf("json path %q not found", path)
	}
	return result.String(), nil
}
