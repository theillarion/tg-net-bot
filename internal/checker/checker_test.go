package checker

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name     string
		method   string
		argument string
		wantM    Method
		wantArg  string
		wantErr  bool
	}{
		{name: "default method", wantM: ICMP},
		{name: "icmp", method: "ICMP", wantM: ICMP},
		{name: "icmp rejects argument", method: "icmp", argument: "1", wantErr: true},
		{name: "tcp default port", method: "tcp", wantM: TCP, wantArg: "443"},
		{name: "tcp custom port", method: "tcp", argument: "22", wantM: TCP, wantArg: "22"},
		{name: "tcp invalid port", method: "tcp", argument: "nope", wantErr: true},
		{name: "tcp zero port", method: "tcp", argument: "0", wantErr: true},
		{name: "tcp port too large", method: "tcp", argument: "65536", wantErr: true},
		{name: "http default", method: "http", wantM: HTTP, wantArg: "https"},
		{name: "http scheme", method: "http", argument: "http", wantM: HTTP, wantArg: "http"},
		{name: "http url", method: "http", argument: "http://example.com/path", wantM: HTTP, wantArg: "http://example.com/path"},
		{name: "http rejects unsupported argument", method: "http", argument: "ftp://example.com", wantErr: true},
		{name: "unknown method", method: "udp", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotM, gotArg, err := Parse(test.method, test.argument)
			if (err != nil) != test.wantErr {
				t.Fatalf("Parse() error = %v, wantErr %v", err, test.wantErr)
			}
			if err == nil && (gotM != test.wantM || gotArg != test.wantArg) {
				t.Fatalf("Parse() = (%q, %q), want (%q, %q)", gotM, gotArg, test.wantM, test.wantArg)
			}
		})
	}
}

func TestValidHost(t *testing.T) {
	tests := []struct {
		name string
		host string
		want bool
	}{
		{name: "domain", host: "example.com", want: true},
		{name: "single-label hostname", host: "localhost", want: true},
		{name: "ipv4", host: "192.0.2.1", want: true},
		{name: "ipv6", host: "2001:db8::1", want: true},
		{name: "bracketed ipv6", host: "[2001:db8::1]", want: true},
		{name: "scoped ipv6", host: "fe80::1%eth0", want: true},
		{name: "empty", host: "", want: false},
		{name: "whitespace", host: "example.com/path", want: false},
		{name: "leading dot", host: ".example.com", want: false},
		{name: "trailing dot", host: "example.com.", want: false},
		{name: "empty label", host: "example..com", want: false},
		{name: "bad label hyphen", host: "-bad.example", want: false},
		{name: "port suffix", host: "example.com:443", want: false},
		{name: "oversized label", host: strings.Repeat("a", 64) + ".com", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := ValidHost(test.host); got != test.want {
				t.Fatalf("ValidHost(%q) = %v, want %v", test.host, got, test.want)
			}
		})
	}
}

func TestServiceCheckHTTP(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		wantUp     bool
	}{
		{name: "success", statusCode: http.StatusOK, wantUp: true},
		{name: "redirect is reachable", statusCode: http.StatusFound, wantUp: true},
		{name: "server error is down", statusCode: http.StatusServiceUnavailable, wantUp: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(test.statusCode) }))
			defer server.Close()
			up, detail := New().Check(context.Background(), "example.com", HTTP, server.URL)
			if up != test.wantUp || !strings.Contains(detail, fmt.Sprint(test.statusCode)) {
				t.Fatalf("Check() = (%v, %q), want up=%v and status %d", up, detail, test.wantUp, test.statusCode)
			}
		})
	}
}

func TestServiceCheckHTTPInvalidAndFailure(t *testing.T) {
	tests := []struct {
		name      string
		argument  string
		serverURL string
		want      string
	}{
		{name: "invalid url", argument: "http://", want: "invalid URL"},
		{name: "request failure", argument: "http", want: "connect:"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := New()
			if test.name == "request failure" {
				server := httptest.NewServer(http.NotFoundHandler())
				test.argument = server.URL
				server.Close()
			}
			up, detail := service.Check(context.Background(), "127.0.0.1", HTTP, test.argument)
			if up || !strings.Contains(detail, test.want) {
				t.Fatalf("Check() = (%v, %q), want down and detail containing %q", up, detail, test.want)
			}
		})
	}
}

func TestServiceCheckTCP(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	up, detail := New().Check(context.Background(), "127.0.0.1", TCP, fmt.Sprint(port))
	if !up || !strings.Contains(detail, fmt.Sprint(port)) {
		t.Fatalf("open TCP port check = (%v, %q)", up, detail)
	}
	if err = listener.Close(); err != nil {
		t.Fatal(err)
	}
	up, detail = New().Check(context.Background(), "127.0.0.1", TCP, fmt.Sprint(port))
	if up || detail == "" {
		t.Fatalf("closed TCP port check = (%v, %q), want failure detail", up, detail)
	}
}

func TestServiceCheckICMP(t *testing.T) {
	dir := t.TempDir()
	ping := filepath.Join(dir, "ping")
	if err := os.WriteFile(ping, []byte("#!/bin/sh\necho fake-ping\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	up, detail := New().Check(context.Background(), "example.com", ICMP, "")
	if !up || detail != "ICMP reply received" {
		t.Fatalf("successful ICMP check = (%v, %q)", up, detail)
	}
	if err := os.WriteFile(ping, []byte("#!/bin/sh\necho unreachable\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	up, detail = New().Check(context.Background(), "example.com", ICMP, "")
	if up || detail != "unreachable" {
		t.Fatalf("failed ICMP check = (%v, %q)", up, detail)
	}
}

func TestServiceCheckUnknownMethodAndHTTPIPv6URL(t *testing.T) {
	if up, detail := New().Check(context.Background(), "example.com", "unknown", ""); up || detail != "unknown check method" {
		t.Fatalf("unknown method result = (%v, %q)", up, detail)
	}
	if got := urlHost("2001:db8::1"); got != "[2001:db8::1]" {
		t.Fatalf("urlHost() = %q", got)
	}
	if got := urlHost("[2001:db8::1]"); got != "[2001:db8::1]" {
		t.Fatalf("urlHost() bracketed = %q", got)
	}
}

func TestHTTPRedirectLimitAndHostValidationEdges(t *testing.T) {
	if ValidHost("bad_name.example") || ValidHost("example.com:443") || ValidHost(strings.Repeat("a", 254)) {
		t.Fatal("invalid hostname accepted")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, r.URL.Path+"x", http.StatusFound) }))
	defer server.Close()
	up, detail := New().Check(context.Background(), "example.com", HTTP, server.URL)
	if !up || !strings.Contains(detail, "302") {
		t.Fatalf("redirect response = %v, %q", up, detail)
	}
	if _, detail = New().Check(context.Background(), "127.0.0.1", TCP, "bad"); detail == "" {
		t.Fatal("invalid TCP port returned no diagnostic")
	}
}
