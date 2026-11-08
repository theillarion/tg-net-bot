package checker

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type Method string

const (
	ICMP Method = "icmp"
	TCP  Method = "tcp"
	HTTP Method = "http"
)

type Service struct{ httpClient *http.Client }

func New() *Service {
	return &Service{httpClient: &http.Client{Timeout: 8 * time.Second, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 5 {
			return http.ErrUseLastResponse
		}
		return nil
	}}}
}

func Parse(method, argument string) (Method, string, error) {
	m := Method(strings.ToLower(method))
	if m == "" {
		m = ICMP
	}
	switch m {
	case ICMP:
		if argument != "" {
			return "", "", fmt.Errorf("icmp does not accept a port")
		}
	case TCP:
		if argument == "" {
			argument = "443"
		}
		p, err := strconv.Atoi(argument)
		if err != nil || p < 1 || p > 65535 {
			return "", "", fmt.Errorf("tcp port must be between 1 and 65535")
		}
	case HTTP:
		if argument == "" {
			argument = "https"
		}
		if argument != "https" && argument != "http" && !strings.HasPrefix(argument, "http://") && !strings.HasPrefix(argument, "https://") {
			return "", "", fmt.Errorf("http check expects http, https, or a URL")
		}
	default:
		return "", "", fmt.Errorf("method must be icmp, tcp, or http")
	}
	return m, argument, nil
}

func ValidHost(host string) bool {
	host = strings.TrimSpace(host)
	if host == "" || strings.ContainsAny(host, " /\t\r\n") {
		return false
	}
	bare := strings.Trim(host, "[]")
	if net.ParseIP(bare) != nil {
		return true
	}
	if i := strings.LastIndexByte(bare, '%'); i > 0 && net.ParseIP(bare[:i]) != nil && i < len(bare)-1 {
		return true
	}
	if len(host) > 253 || strings.HasPrefix(host, ".") || strings.HasSuffix(host, ".") || strings.Contains(host, ":") {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
			return false
		}
		for _, r := range label {
			if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-') {
				return false
			}
		}
	}
	return true
}

func (s *Service) Check(ctx context.Context, host string, method Method, argument string) (bool, string) {
	switch method {
	case ICMP:
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		cmd := exec.CommandContext(pingCtx, "ping", "-c", "1", "-W", "4", host)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return false, strings.TrimSpace(string(out))
		}
		return true, "ICMP reply received"
	case TCP:
		address := net.JoinHostPort(strings.Trim(host, "[]"), argument)
		d := net.Dialer{Timeout: 5 * time.Second}
		conn, err := d.DialContext(ctx, "tcp", address)
		if err != nil {
			return false, err.Error()
		}
		if err = conn.Close(); err != nil {
			return false, fmt.Sprintf("close connection: %v", err)
		}
		return true, "TCP connection established on port " + argument
	case HTTP:
		endpoint := argument
		if endpoint == "http" || endpoint == "https" {
			endpoint = endpoint + "://" + urlHost(host)
		} else if !strings.Contains(endpoint, "://") {
			endpoint = "https://" + endpoint
		}
		u, err := url.Parse(endpoint)
		if err != nil || u.Host == "" {
			return false, "invalid URL"
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return false, err.Error()
		}
		res, err := s.httpClient.Do(req)
		if err != nil {
			return false, err.Error()
		}
		defer res.Body.Close()
		if res.StatusCode >= 500 {
			return false, "HTTP " + res.Status
		}
		return true, "HTTP " + res.Status
	default:
		return false, "unknown check method"
	}
}

func urlHost(host string) string {
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		return "[" + host + "]"
	}
	return host
}
