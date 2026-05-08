package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type TelegramClient struct {
	token      string
	baseURL    string
	httpClient *http.Client
}

func NewTelegramClient(token string) *TelegramClient {
	return &TelegramClient{
		token:   token,
		baseURL: "https://api.telegram.org",
		httpClient: &http.Client{
			Timeout: 35 * time.Second,
		},
	}
}

type TelegramUser struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
}

type TelegramChat struct {
	ID int64 `json:"id"`
}

type TelegramMessage struct {
	MessageID int          `json:"message_id"`
	From      TelegramUser `json:"from"`
	Chat      TelegramChat `json:"chat"`
	Text      string       `json:"text"`
}

type TelegramCallbackQuery struct {
	ID      string           `json:"id"`
	From    TelegramUser     `json:"from"`
	Message *TelegramMessage `json:"message,omitempty"`
	Data    string           `json:"data"`
}

type TelegramUpdate struct {
	UpdateID      int64                  `json:"update_id"`
	Message       *TelegramMessage       `json:"message,omitempty"`
	CallbackQuery *TelegramCallbackQuery `json:"callback_query,omitempty"`
}

type apiResponse[T any] struct {
	OK          bool   `json:"ok"`
	Description string `json:"description,omitempty"`
	ErrorCode   int    `json:"error_code,omitempty"`
	Parameters  struct {
		RetryAfter int `json:"retry_after,omitempty"`
	} `json:"parameters,omitempty"`
	Result T `json:"result"`
}

type InlineKeyboardMarkup struct {
	InlineKeyboard [][]InlineKeyboardButton `json:"inline_keyboard"`
}

type InlineKeyboardButton struct {
	Text         string `json:"text"`
	CallbackData string `json:"callback_data,omitempty"`
}

func (c *TelegramClient) GetUpdates(ctx context.Context, offset int64, limit int, timeoutSeconds int) ([]TelegramUpdate, error) {
	q := url.Values{}
	if offset > 0 {
		q.Set("offset", strconv.FormatInt(offset, 10))
	}
	q.Set("limit", strconv.Itoa(limit))
	q.Set("timeout", strconv.Itoa(timeoutSeconds))
	endpoint := fmt.Sprintf("%s/bot%s/getUpdates?%s", c.baseURL, c.token, q.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	var resp apiResponse[[]TelegramUpdate]
	if err := c.do(req, &resp); err != nil {
		return nil, err
	}
	return resp.Result, nil
}

func (c *TelegramClient) SendMessage(ctx context.Context, chatID int64, text string) error {
	return c.SendMessageWithKeyboard(ctx, chatID, text, nil)
}

func (c *TelegramClient) SendMessageWithKeyboard(ctx context.Context, chatID int64, text string, kb *InlineKeyboardMarkup) error {
	if strings.TrimSpace(text) == "" {
		return nil
	}
	body := map[string]any{
		"chat_id":                  chatID,
		"text":                     text,
		"parse_mode":               "HTML",
		"disable_web_page_preview": true,
	}
	if kb != nil {
		body["reply_markup"] = kb
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	endpoint := fmt.Sprintf("%s/bot%s/sendMessage", c.baseURL, c.token)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	var resp apiResponse[json.RawMessage]
	return c.doWithRetry(ctx, req, &resp)
}

func (c *TelegramClient) EditMessageText(ctx context.Context, chatID int64, messageID int, text string, kb *InlineKeyboardMarkup) error {
	if strings.TrimSpace(text) == "" {
		return nil
	}
	body := map[string]any{
		"chat_id":                  chatID,
		"message_id":               messageID,
		"text":                     text,
		"parse_mode":               "HTML",
		"disable_web_page_preview": true,
	}
	if kb != nil {
		body["reply_markup"] = kb
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	endpoint := fmt.Sprintf("%s/bot%s/editMessageText", c.baseURL, c.token)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	var resp apiResponse[json.RawMessage]
	if err := c.doWithRetry(ctx, req, &resp); err != nil {
		if IsIgnorableTelegramUIError(err) {
			return nil
		}
		return err
	}
	return nil
}

func (c *TelegramClient) AnswerCallbackQuery(ctx context.Context, callbackID string, text string) error {
	body := map[string]any{
		"callback_query_id": callbackID,
	}
	if strings.TrimSpace(text) != "" {
		body["text"] = text
		body["show_alert"] = false
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	endpoint := fmt.Sprintf("%s/bot%s/answerCallbackQuery", c.baseURL, c.token)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	var resp apiResponse[json.RawMessage]
	return c.doWithRetry(ctx, req, &resp)
}

func (c *TelegramClient) do(req *http.Request, target any) error {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
		return fmt.Errorf("decode telegram response: %w", err)
	}
	if resp.StatusCode >= 300 {
		switch r := target.(type) {
		case *apiResponse[[]TelegramUpdate]:
			if r.Parameters.RetryAfter > 0 {
				return fmt.Errorf("telegram retry_after %d", r.Parameters.RetryAfter)
			}
		case *apiResponse[json.RawMessage]:
			if r.Parameters.RetryAfter > 0 {
				return fmt.Errorf("telegram retry_after %d", r.Parameters.RetryAfter)
			}
		}
		switch r := target.(type) {
		case *apiResponse[[]TelegramUpdate]:
			if strings.TrimSpace(r.Description) != "" {
				return fmt.Errorf("telegram http status %d: %s", resp.StatusCode, r.Description)
			}
		case *apiResponse[json.RawMessage]:
			if strings.TrimSpace(r.Description) != "" {
				return fmt.Errorf("telegram http status %d: %s", resp.StatusCode, r.Description)
			}
		}
		return fmt.Errorf("telegram http status %d", resp.StatusCode)
	}
	switch r := target.(type) {
	case *apiResponse[[]TelegramUpdate]:
		if !r.OK {
			if r.Parameters.RetryAfter > 0 {
				return fmt.Errorf("telegram retry_after %d", r.Parameters.RetryAfter)
			}
			return fmt.Errorf("telegram api error: %s", r.Description)
		}
	case *apiResponse[json.RawMessage]:
		if !r.OK {
			if r.Parameters.RetryAfter > 0 {
				return fmt.Errorf("telegram retry_after %d", r.Parameters.RetryAfter)
			}
			return fmt.Errorf("telegram api error: %s", r.Description)
		}
	}
	return nil
}

func IsIgnorableTelegramUIError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "message is not modified") ||
		strings.Contains(msg, "message text is empty")
}

func (c *TelegramClient) doWithRetry(ctx context.Context, req *http.Request, target any) error {
	var attempt int
	for {
		attempt++
		clonedReq, err := cloneRequest(ctx, req)
		if err != nil {
			return err
		}
		err = c.do(clonedReq, target)
		if err == nil {
			return nil
		}
		if d := retryAfterFromErr(err); d > 0 {
			if attempt >= 5 {
				return err
			}
			sleep(ctx, d)
			continue
		}
		if attempt >= 3 {
			return err
		}
		sleep(ctx, time.Duration(attempt)*250*time.Millisecond)
	}
}

func retryAfterFromErr(err error) time.Duration {
	if err == nil {
		return 0
	}
	msg := err.Error()
	const prefix = "telegram retry_after "
	if !strings.HasPrefix(msg, prefix) {
		return 0
	}
	secStr := strings.TrimSpace(strings.TrimPrefix(msg, prefix))
	sec, parseErr := strconv.Atoi(secStr)
	if parseErr != nil || sec <= 0 {
		return 0
	}
	return time.Duration(sec) * time.Second
}

func cloneRequest(ctx context.Context, req *http.Request) (*http.Request, error) {
	var bodyBytes []byte
	if req.Body != nil {
		b, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		_ = req.Body.Close()
		bodyBytes = b
	}
	clone, err := http.NewRequestWithContext(ctx, req.Method, req.URL.String(), bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}
	clone.Header = req.Header.Clone()
	return clone, nil
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
