package cheburcheck

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct{ httpClient *http.Client }

func New() *Client { return &Client{httpClient: &http.Client{Timeout: 90 * time.Second}} }

func (c *Client) Check(ctx context.Context, target string) (string, error) {
	endpoint := "https://cheburcheck.ru/api/v1/check?target=" + url.QueryEscape(target)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("create cheburcheck request: %w", err)
	}
	res, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("call cheburcheck: %w", err)
	}
	body, readErr := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	closeErr := res.Body.Close()
	if readErr != nil {
		return "", fmt.Errorf("read cheburcheck response: %w", readErr)
	}
	if closeErr != nil {
		return "", fmt.Errorf("close cheburcheck response: %w", closeErr)
	}
	if res.StatusCode/100 != 2 {
		return "", fmt.Errorf("cheburcheck returned HTTP %s", res.Status)
	}
	var payload map[string]any
	if err = json.Unmarshal(body, &payload); err != nil {
		return "", fmt.Errorf("decode cheburcheck response: %w", err)
	}
	baseVerdict := ""
	for _, field := range []string{"blocked", "is_blocked", "blocklisted"} {
		if value, ok := payload[field].(bool); ok {
			if value {
				baseVerdict = "database: listed as blocked"
			} else {
				baseVerdict = "database: not found in blocklists"
			}
			break
		}
	}
	id := ""
	if value, ok := payload["id"]; ok && value != nil {
		id = fmt.Sprint(value)
	}
	if id == "" {
		if baseVerdict != "" {
			return baseVerdict, nil
		}
		return compact(string(body)), nil
	}
	probeReq, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://cheburcheck.ru/api/v1/probe/"+url.PathEscape(id), nil)
	if err != nil {
		return "", fmt.Errorf("create cheburcheck probe request: %w", err)
	}
	probeRes, err := c.httpClient.Do(probeReq)
	if err != nil {
		if baseVerdict != "" {
			return baseVerdict + "; probe unavailable: " + err.Error(), nil
		}
		return "", fmt.Errorf("call cheburcheck probe: %w", err)
	}
	defer probeRes.Body.Close()
	if probeRes.StatusCode/100 != 2 {
		if baseVerdict != "" {
			return baseVerdict + "; probe returned HTTP " + probeRes.Status, nil
		}
		return "", fmt.Errorf("cheburcheck probe returned HTTP %s", probeRes.Status)
	}
	scanner := bufio.NewScanner(io.LimitReader(probeRes.Body, 2<<20))
	scanner.Buffer(make([]byte, 4096), 1<<20)
	last := ""
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, ":") || strings.HasPrefix(line, "event:") || strings.HasPrefix(line, "id:") || strings.HasPrefix(line, "retry:") {
			continue
		}
		line = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if line != "" {
			last = line
			if strings.Contains(strings.ToLower(line), "done") || strings.Contains(strings.ToLower(line), "complete") {
				break
			}
		}
	}
	if err = scanner.Err(); err != nil {
		if baseVerdict != "" {
			return baseVerdict + "; probe read error: " + err.Error(), nil
		}
		return "", fmt.Errorf("read cheburcheck probe: %w", err)
	}
	if last == "" {
		if baseVerdict != "" {
			return baseVerdict, nil
		}
		return "", fmt.Errorf("cheburcheck probe returned no result")
	}
	probeVerdict := interpret(last)
	if baseVerdict != "" {
		return baseVerdict + "; probe: " + probeVerdict, nil
	}
	return probeVerdict, nil
}

func compact(s string) string {
	if len(s) > 500 {
		return s[:500] + "…"
	}
	return s
}
func interpret(raw string) string {
	var v any
	if json.Unmarshal([]byte(raw), &v) != nil {
		return compact(raw)
	}
	parts := []string{}
	var walk func(any)
	walk = func(x any) {
		switch item := x.(type) {
		case map[string]any:
			for k, val := range item {
				if strings.Contains(strings.ToLower(k), "block") || strings.Contains(strings.ToLower(k), "status") || strings.Contains(strings.ToLower(k), "result") || strings.Contains(strings.ToLower(k), "message") {
					parts = append(parts, fmt.Sprintf("%s=%v", k, val))
				}
				walk(val)
			}
		case []any:
			for _, val := range item {
				walk(val)
			}
		}
	}
	walk(v)
	if len(parts) > 0 {
		return compact(strings.Join(parts, "; "))
	}
	return compact(raw)
}
