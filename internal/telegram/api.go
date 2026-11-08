package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type API struct {
	token  string
	client *http.Client
}

func NewAPI(token, proxy string) (*API, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if proxy != "" {
		u, err := url.Parse(proxy)
		if err != nil {
			return nil, fmt.Errorf("parse Telegram proxy URL: %w", err)
		}
		transport.Proxy = http.ProxyURL(u)
	}
	return &API{token: token, client: &http.Client{Transport: transport, Timeout: 70 * time.Second}}, nil
}
func (a *API) Call(ctx context.Context, method string, payload any, result any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode Telegram request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.telegram.org/bot"+a.token+"/"+method, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create Telegram request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("call Telegram %s: %w", method, err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 2<<20))
	if err != nil {
		return fmt.Errorf("read Telegram response: %w", err)
	}
	var envelope struct {
		OK          bool            `json:"ok"`
		Description string          `json:"description"`
		Result      json.RawMessage `json:"result"`
	}
	if err = json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("decode Telegram response: %w", err)
	}
	if !envelope.OK {
		return fmt.Errorf("Telegram %s: %s", method, envelope.Description)
	}
	if result != nil {
		if err = json.Unmarshal(envelope.Result, result); err != nil {
			return fmt.Errorf("decode Telegram %s result: %w", method, err)
		}
	}
	return nil
}
func (a *API) Send(ctx context.Context, chatID int64, text string) error {
	return a.Call(ctx, "sendMessage", map[string]any{"chat_id": chatID, "text": text}, nil)
}

func (a *API) SendWithMarkup(ctx context.Context, chatID int64, text string, markup any) error {
	return a.Call(ctx, "sendMessage", map[string]any{"chat_id": chatID, "text": text, "reply_markup": markup}, nil)
}

func (a *API) AnswerCallback(ctx context.Context, callbackID, text string) error {
	return a.Call(ctx, "answerCallbackQuery", map[string]any{"callback_query_id": callbackID, "text": text}, nil)
}
