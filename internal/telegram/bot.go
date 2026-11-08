package telegram

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/theillarion/tg-net-bot/internal/checker"
	"github.com/theillarion/tg-net-bot/internal/domain"
	"github.com/theillarion/tg-net-bot/internal/monitor"
)

const (
	buttonCheck   = "🔎 Проверить доступность"
	buttonBlock   = "🚫 Проверить блокировку"
	buttonAdd     = "➕ Добавить сервер"
	buttonTargets = "🖥 Мои серверы"
	buttonNewRule = "🔔 Настроить алерт"
	buttonRules   = "📋 Мои алерты"
	buttonHelp    = "❓ Помощь"
)

type interaction = domain.Interaction

type InteractionStore interface {
	LoadInteractions(context.Context) (map[int64]domain.Interaction, error)
	SaveInteraction(context.Context, int64, domain.Interaction) error
	DeleteInteraction(context.Context, int64) error
}

type Bot struct {
	pendingMu sync.Mutex
	queuesMu  sync.Mutex
	api       *API
	service   *monitor.Service
	allowed   map[int64]struct{}
	store     InteractionStore
	pending   map[int64]interaction
	queues    map[int64]chan Update
	logger    *slog.Logger
}

type Update struct {
	UpdateID      int            `json:"update_id"`
	Message       *Message       `json:"message"`
	CallbackQuery *CallbackQuery `json:"callback_query"`
}
type User struct {
	ID int64 `json:"id"`
}
type Chat struct {
	ID int64 `json:"id"`
}
type Message struct {
	Text string `json:"text"`
	Chat Chat   `json:"chat"`
	From User   `json:"from"`
}
type CallbackQuery struct {
	ID      string   `json:"id"`
	From    User     `json:"from"`
	Message *Message `json:"message"`
	Data    string   `json:"data"`
}
type KeyboardButton struct {
	Text string `json:"text"`
}
type InlineButton struct {
	Text         string `json:"text"`
	CallbackData string `json:"callback_data"`
}

func NewBot(api *API, service *monitor.Service, allowed map[int64]struct{}, logger *slog.Logger, stores ...InteractionStore) *Bot {
	if logger == nil {
		logger = slog.Default()
	}
	var store InteractionStore
	if len(stores) > 0 {
		store = stores[0]
	}
	return &Bot{api: api, service: service, allowed: allowed, store: store, pending: make(map[int64]interaction), queues: make(map[int64]chan Update), logger: logger}
}

func (b *Bot) Run(ctx context.Context) error {
	if b.store != nil {
		pending, err := b.store.LoadInteractions(ctx)
		if err != nil {
			return fmt.Errorf("restore Telegram dialog state: %w", err)
		}
		b.pendingMu.Lock()
		for userID, state := range pending {
			b.pending[userID] = state
		}
		b.pendingMu.Unlock()
	}
	offset := 0
	connected := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var updates []Update
		err := b.api.Call(ctx, "getUpdates", map[string]any{"offset": offset, "timeout": 50, "allowed_updates": []string{"message", "callback_query"}}, &updates)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			connected = false
			b.logger.Error("Telegram polling failed", "error", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(3 * time.Second):
				continue
			}
		}
		if !connected {
			b.logger.Info("Telegram polling connected")
			connected = true
		}
		for _, update := range updates {
			offset = update.UpdateID + 1
			userID := update.userID()
			if _, ok := b.allowed[userID]; ok {
				b.enqueue(ctx, userID, update)
			}
		}
	}
}

func (u Update) userID() int64 {
	if u.Message != nil {
		return u.Message.From.ID
	}
	if u.CallbackQuery != nil {
		return u.CallbackQuery.From.ID
	}
	return 0
}

func (b *Bot) enqueue(ctx context.Context, userID int64, update Update) {
	b.queuesMu.Lock()
	queue := b.queues[userID]
	if queue == nil {
		queue = make(chan Update, 32)
		b.queues[userID] = queue
		go b.runQueue(ctx, queue)
	}
	b.queuesMu.Unlock()
	select {
	case queue <- update:
	case <-ctx.Done():
	}
}

func (b *Bot) runQueue(ctx context.Context, queue <-chan Update) {
	for {
		select {
		case <-ctx.Done():
			return
		case update := <-queue:
			if update.CallbackQuery != nil {
				b.handleCallback(ctx, *update.CallbackQuery)
			} else if update.Message != nil {
				b.handleMessage(ctx, *update.Message)
			}
		}
	}
}

func (b *Bot) handleMessage(ctx context.Context, message Message) {
	userID, chatID := message.From.ID, message.Chat.ID
	input := strings.TrimSpace(message.Text)
	if input == "" {
		return
	}
	if b.handleMenuButton(ctx, chatID, userID, input) {
		return
	}
	b.pendingMu.Lock()
	pending, hasPending := b.pending[userID]
	if hasPending && !strings.HasPrefix(input, "/") {
		b.pendingMu.Unlock()
		b.handlePending(ctx, chatID, userID, pending, input)
		return
	}
	b.pendingMu.Unlock()

	parts := strings.Fields(input)
	command := strings.TrimPrefix(parts[0], "/")
	if i := strings.IndexByte(command, '@'); i >= 0 {
		command = command[:i]
	}
	args := parts[1:]
	switch command {
	case "start", "help":
		b.sendMenu(ctx, chatID, "Выбери действие кнопкой. Команды тоже работают: /check, /block, /add, /hosts, /alert, /alerts.")
	case "check":
		b.manualCheckCommand(ctx, chatID, userID, args)
	case "block":
		b.manualBlockCommand(ctx, chatID, userID, args)
	case "add":
		if len(args) == 2 {
			b.addTarget(ctx, chatID, userID, args[0], args[1])
			return
		}
		if !b.setPending(ctx, chatID, userID, interaction{Step: "add-name"}) {
			return
		}
		b.send(ctx, chatID, "Как назвать сервер?")
	case "hosts":
		b.listTargets(ctx, chatID, userID)
	case "remove":
		if len(args) != 1 {
			b.send(ctx, chatID, "Использование: /remove <имя>")
			return
		}
		if err := b.service.RemoveTarget(ctx, userID, args[0]); err != nil {
			b.send(ctx, chatID, "Не удалось удалить сервер: "+err.Error())
			return
		}
		b.send(ctx, chatID, "Сервер и его алерты удалены.")
	case "alert":
		b.addAlertCommand(ctx, chatID, userID, args)
	case "alerts":
		b.listAlerts(ctx, chatID, userID)
	case "alert_remove":
		if len(args) != 1 {
			b.send(ctx, chatID, "Использование: /alert_remove <ID>")
			return
		}
		if err := b.service.RemoveAlert(ctx, userID, args[0]); err != nil {
			b.send(ctx, chatID, "Не удалось удалить алерт: "+err.Error())
			return
		}
		b.send(ctx, chatID, "Алерт удалён.")
	default:
		b.send(ctx, chatID, "Нажми /start, чтобы открыть меню.")
	}
}

func (b *Bot) handleMenuButton(ctx context.Context, chatID, userID int64, input string) bool {
	switch input {
	case buttonCheck, buttonBlock, buttonAdd, buttonTargets, buttonNewRule, buttonRules, buttonHelp:
	default:
		return false
	}
	switch input {
	case buttonCheck:
		if !b.setPending(ctx, chatID, userID, interaction{Step: "check-target"}) {
			return true
		}
		b.send(ctx, chatID, "Укажи сервер по IP, домену или имени сохранённого сервера для проверки доступности.")
	case buttonBlock:
		if !b.setPending(ctx, chatID, userID, interaction{Step: "block-target"}) {
			return true
		}
		b.send(ctx, chatID, "Укажи сервер по IP, домену или имени сохранённого сервера для проверки в Cheburcheck.")
	case buttonAdd:
		if !b.setPending(ctx, chatID, userID, interaction{Step: "add-name"}) {
			return true
		}
		b.send(ctx, chatID, "Как назвать сервер?")
	case buttonTargets:
		b.listTargets(ctx, chatID, userID)
	case buttonNewRule:
		b.chooseAlertTarget(ctx, chatID, userID)
	case buttonRules:
		b.listAlerts(ctx, chatID, userID)
	case buttonHelp:
		b.sendMenu(ctx, chatID, helpText)
	default:
		return false
	}
	return true
}

func (b *Bot) handlePending(ctx context.Context, chatID, userID int64, pending interaction, value string) {
	value = strings.TrimSpace(value)
	switch pending.Step {
	case "add-name":
		if value == "" {
			b.send(ctx, chatID, "Имя не может быть пустым. Нажми «Добавить сервер» и попробуй ещё раз.")
			return
		}
		pending.Name, pending.Step = value, "add-host"
		if !b.setPending(ctx, chatID, userID, pending) {
			return
		}
		b.send(ctx, chatID, "Теперь укажи сервер: IP или домен.")
	case "add-host":
		if err := b.service.AddTarget(ctx, userID, pending.Name, value); err != nil {
			if existing, ok := b.service.FindTarget(userID, pending.Name); !ok || !strings.EqualFold(existing.Host, value) {
				if ok {
					if !b.setPending(ctx, chatID, userID, interaction{Step: "add-name"}) {
						return
					}
					b.send(ctx, chatID, "Имя уже занято. Введи другое имя сервера.")
					return
				}
				b.send(ctx, chatID, "Не удалось добавить сервер: "+err.Error()+" Попробуй указать IP или домен ещё раз.")
				return
			}
		}
		if !b.clearPending(ctx, chatID, userID) {
			b.send(ctx, chatID, "Сервер сохранён, но форму не удалось завершить. Проверь список серверов перед повтором.")
			return
		}
		b.send(ctx, chatID, fmt.Sprintf("Сохранил «%s» — %s. Открой «Мои серверы», чтобы проверить его или настроить алерт.", pending.Name, value))
	case "check-target":
		target := b.resolveTarget(userID, value)
		if target == "" {
			b.send(ctx, chatID, "Не нашёл сервер с таким именем. Проверь IP или домен и попробуй снова.")
			return
		}
		if !b.setPending(ctx, chatID, userID, interaction{Step: "check-method", Host: target}) {
			return
		}
		b.sendInline(ctx, chatID, "Каким способом проверить доступность?", methodKeyboard("check-method"))
	case "manual-tcp-port":
		if _, _, err := checker.Parse("tcp", value); err != nil {
			b.send(ctx, chatID, err.Error()+" Отправь порт от 1 до 65535.")
			return
		}
		b.runManualCheck(ctx, chatID, pending.Host, "tcp", value)
		b.clearPending(ctx, chatID, userID)
	case "block-target":
		target := b.resolveTarget(userID, value)
		if target == "" {
			b.send(ctx, chatID, "Не нашёл IP, домен или сохранённый сервер с таким именем.")
			return
		}
		b.manualBlock(ctx, chatID, target)
		b.clearPending(ctx, chatID, userID)
	case "add-alert-interval":
		interval, err := time.ParseDuration(value)
		if err != nil || interval <= 0 {
			b.send(ctx, chatID, "Нужен положительный интервал, например 30s, 5m или 2h. Попробуй ещё раз.")
			return
		}
		b.createAlert(ctx, chatID, userID, pending, interval)
	case "add-alert-tcp-port":
		if _, _, err := checker.Parse("tcp", value); err != nil {
			b.send(ctx, chatID, err.Error()+" Попробуй ещё раз.")
			return
		}
		pending.Argument = value
		pending.Step = "add-alert-interval"
		if !b.setPending(ctx, chatID, userID, pending) {
			return
		}
		b.send(ctx, chatID, "Как часто проверять этот алерт? Например: 1m, 15m, 2h.")
	}
}

func (b *Bot) handleCallback(ctx context.Context, callback CallbackQuery) {
	chatID := int64(0)
	if callback.Message != nil {
		chatID = callback.Message.Chat.ID
	}
	if err := b.api.AnswerCallback(ctx, callback.ID, "Готово"); err != nil {
		b.logger.Error("answer Telegram callback", "error", err)
	}
	if chatID == 0 {
		return
	}
	parts := strings.Split(callback.Data, ":")
	if len(parts) < 2 {
		return
	}
	switch parts[0] {
	case "check-method":
		b.selectManualMethod(ctx, chatID, callback.From.ID, parts[1])
	case "manual-http":
		pending := b.getPending(callback.From.ID)
		if pending.Step != "manual-http" || !checker.ValidHost(pending.Host) {
			b.send(ctx, chatID, "Это меню проверки уже устарело. Начни проверку заново кнопкой «Проверить доступность».")
			return
		}
		b.runManualCheck(ctx, chatID, pending.Host, "http", parts[1])
		b.clearPending(ctx, chatID, callback.From.ID)
	case "alert-target":
		b.selectAlertTarget(ctx, chatID, callback.From.ID, parts[1])
	case "alert-method":
		b.selectAlertMethod(ctx, chatID, callback.From.ID, parts[1])
	case "alert-http":
		pending := b.getPending(callback.From.ID)
		if pending.Step != "alert-http" || !checker.ValidHost(pending.Host) {
			b.send(ctx, chatID, "Это меню настройки алерта уже устарело. Открой «Настроить алерт» и продолжи.")
			return
		}
		pending.Method, pending.Argument = "http", parts[1]
		b.askAlertInterval(ctx, chatID, callback.From.ID, pending)
	case "target-check", "target-block", "target-alert", "target-remove":
		b.handleTargetAction(ctx, chatID, callback.From.ID, parts[0], at(parts, 1))
	case "alert-remove":
		if err := b.service.RemoveAlert(ctx, callback.From.ID, at(parts, 1)); err != nil {
			b.send(ctx, chatID, "Не удалось удалить алерт: "+err.Error())
			return
		}
		b.listAlerts(ctx, chatID, callback.From.ID)
	}
}

func (b *Bot) selectManualMethod(ctx context.Context, chatID, userID int64, method string) {
	pending := b.getPending(userID)
	if pending.Step != "check-method" || !checker.ValidHost(pending.Host) {
		b.send(ctx, chatID, "Это меню проверки уже устарело. Начни проверку заново кнопкой «Проверить доступность».")
		return
	}
	switch method {
	case "icmp":
		b.runManualCheck(ctx, chatID, pending.Host, method, "")
		b.clearPending(ctx, chatID, userID)
	case "tcp":
		pending.Step = "manual-tcp-port"
		if !b.setPending(ctx, chatID, userID, pending) {
			return
		}
		b.send(ctx, chatID, "Какой TCP-порт проверить? Отправь номер, например 443.")
	case "http":
		pending.Step = "manual-http"
		if !b.setPending(ctx, chatID, userID, pending) {
			return
		}
		b.sendInline(ctx, chatID, "Проверить HTTPS или HTTP?", [][]InlineButton{{{Text: "HTTPS", CallbackData: "manual-http:https"}, {Text: "HTTP", CallbackData: "manual-http:http"}}})
	default:
		b.send(ctx, chatID, "Неизвестный способ проверки. Начни проверку заново.")
	}
}

func (b *Bot) selectAlertTarget(ctx context.Context, chatID, userID int64, rawIndex string) {
	if pending := b.getPending(userID); pending.Step != "alert-target" {
		b.send(ctx, chatID, "Это меню выбора сервера для алерта уже устарело. Открой «Настроить алерт» и продолжи.")
		return
	}
	index, err := strconv.Atoi(rawIndex)
	targets := b.service.Targets(userID)
	if err != nil || index < 0 || index >= len(targets) {
		b.send(ctx, chatID, "Не получилось выбрать сервер. Открой «Настроить алерт» ещё раз.")
		return
	}
	target := targets[index]
	if !b.setPending(ctx, chatID, userID, interaction{Step: "alert-method", Name: target.Name, Host: target.Host, RequestID: monitor.NewAlertID()}) {
		return
	}
	b.sendInline(ctx, chatID, fmt.Sprintf("Как проверять «%s»?", target.Name), methodKeyboard("alert-method"))
}

func (b *Bot) selectAlertMethod(ctx context.Context, chatID, userID int64, method string) {
	pending := b.getPending(userID)
	if pending.Step != "alert-method" || !checker.ValidHost(pending.Host) || pending.Name == "" {
		b.send(ctx, chatID, "Это меню настройки алерта уже устарело. Открой «Настроить алерт» и продолжи.")
		return
	}
	pending.Method = method
	switch method {
	case "icmp":
		b.askAlertInterval(ctx, chatID, userID, pending)
	case "tcp":
		pending.Step = "add-alert-tcp-port"
		if !b.setPending(ctx, chatID, userID, pending) {
			return
		}
		b.send(ctx, chatID, "Какой TCP-порт проверять? Например, 22 или 443.")
	case "http":
		pending.Step = "alert-http"
		if !b.setPending(ctx, chatID, userID, pending) {
			return
		}
		b.sendInline(ctx, chatID, "Проверять HTTP или HTTPS?", [][]InlineButton{{{Text: "HTTPS", CallbackData: "alert-http:https"}, {Text: "HTTP", CallbackData: "alert-http:http"}}})
	default:
		b.send(ctx, chatID, "Неизвестный способ проверки.")
	}
}

func (b *Bot) askAlertInterval(ctx context.Context, chatID, userID int64, pending interaction) {
	pending.Step = "add-alert-interval"
	if !b.setPending(ctx, chatID, userID, pending) {
		return
	}
	b.send(ctx, chatID, "Как часто запускать алерт для этого сервера? Например: 1m, 15m, 2h.")
}

func (b *Bot) createAlert(ctx context.Context, chatID, userID int64, pending interaction, interval time.Duration) {
	if pending.RequestID == "" {
		pending.RequestID = monitor.NewAlertID()
		if !b.setPending(ctx, chatID, userID, pending) {
			return
		}
	}
	alert, err := b.service.AddAlertWithID(ctx, pending.RequestID, chatID, userID, pending.Name, pending.Method, pending.Argument, interval)
	if err != nil {
		b.send(ctx, chatID, "Не удалось создать алерт: "+err.Error())
		return
	}
	if !b.clearPending(ctx, chatID, userID) {
		b.send(ctx, chatID, "Алерт создан, но состояние формы не удалось очистить. Проверь список алертов перед повторной настройкой.")
		return
	}
	b.send(ctx, chatID, fmt.Sprintf("Алерт настроен для «%s»: %s, каждые %s. При недоступности проверю Cheburcheck.", alert.Target.Name, alert.Method, alert.Interval))
}

func (b *Bot) handleTargetAction(ctx context.Context, chatID, userID int64, action, rawIndex string) {
	index, err := strconv.Atoi(rawIndex)
	targets := b.service.Targets(userID)
	if err != nil || index < 0 || index >= len(targets) {
		b.send(ctx, chatID, "Список серверов изменился. Открой «Мои серверы» ещё раз.")
		return
	}
	target := targets[index]
	switch action {
	case "target-check":
		if !b.setPending(ctx, chatID, userID, interaction{Step: "check-method", Host: target.Host}) {
			return
		}
		b.sendInline(ctx, chatID, fmt.Sprintf("Как проверить «%s»?", target.Name), methodKeyboard("check-method"))
	case "target-block":
		b.manualBlock(ctx, chatID, target.Host)
	case "target-alert":
		if !b.setPending(ctx, chatID, userID, interaction{Step: "alert-method", Name: target.Name, Host: target.Host, RequestID: monitor.NewAlertID()}) {
			return
		}
		b.sendInline(ctx, chatID, fmt.Sprintf("Как проверять «%s»?", target.Name), methodKeyboard("alert-method"))
	case "target-remove":
		if err := b.service.RemoveTarget(ctx, userID, target.Name); err != nil {
			b.send(ctx, chatID, "Не удалось удалить сервер: "+err.Error())
			return
		}
		b.listTargets(ctx, chatID, userID)
	}
}

const helpText = "Выбери действие кнопкой в меню:\n\n🔎 Проверить доступность — разовая проверка по ICMP, TCP или HTTP/HTTPS.\n🚫 Проверить блокировку — разовый запрос в Cheburcheck.\n➕ Добавить сервер — сохранить IP или домен.\n🖥 Мои серверы — проверить, запросить блокировку, создать алерт или удалить сервер.\n🔔 Настроить алерт — выбрать сервер, способ проверки и периодичность.\n📋 Мои алерты — посмотреть и удалить правила.\n\nДля проверки можно вводить IPv4, IPv6 или домен. Команды /check, /block, /add и /alert также доступны."

func (b *Bot) send(ctx context.Context, chatID int64, text string) {
	if err := b.api.Send(ctx, chatID, text); err != nil {
		b.logger.Error("send Telegram message", "chat_id", chatID, "error", err)
	}
}
func (b *Bot) sendMenu(ctx context.Context, chatID int64, text string) {
	markup := map[string]any{
		"keyboard": [][]KeyboardButton{
			{{Text: buttonCheck}, {Text: buttonBlock}},
			{{Text: buttonAdd}, {Text: buttonTargets}},
			{{Text: buttonNewRule}, {Text: buttonRules}},
			{{Text: buttonHelp}},
		},
		"resize_keyboard":         true,
		"is_persistent":           true,
		"input_field_placeholder": "Выбери действие",
	}
	if err := b.api.SendWithMarkup(ctx, chatID, text, markup); err != nil {
		b.logger.Error("send Telegram menu", "chat_id", chatID, "error", err)
	}
}
func (b *Bot) sendInline(ctx context.Context, chatID int64, text string, keyboard [][]InlineButton) {
	if err := b.api.SendWithMarkup(ctx, chatID, text, map[string]any{"inline_keyboard": keyboard}); err != nil {
		b.logger.Error("send Telegram inline keyboard", "chat_id", chatID, "error", err)
	}
}
func methodKeyboard(prefix string) [][]InlineButton {
	return [][]InlineButton{{{Text: "ICMP ping", CallbackData: prefix + ":icmp"}, {Text: "TCP порт", CallbackData: prefix + ":tcp"}}, {{Text: "HTTP/HTTPS", CallbackData: prefix + ":http"}}}
}

func (b *Bot) setPending(ctx context.Context, chatID, userID int64, state interaction) bool {
	if b.store != nil {
		if err := b.store.SaveInteraction(ctx, userID, state); err != nil {
			b.logger.Error("persist Telegram dialog state", "user_id", userID, "step", state.Step, "error", err)
			b.send(ctx, chatID, "Не удалось сохранить состояние формы. Данные не продвинуты, попробуй ещё раз.")
			return false
		}
	}
	b.pendingMu.Lock()
	b.pending[userID] = state
	b.pendingMu.Unlock()
	return true
}
func (b *Bot) getPending(userID int64) interaction {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()
	return b.pending[userID]
}
func (b *Bot) clearPending(ctx context.Context, chatID, userID int64) bool {
	if b.store != nil {
		if err := b.store.DeleteInteraction(ctx, userID); err != nil {
			b.logger.Error("clear persisted Telegram dialog state", "user_id", userID, "error", err)
			b.send(ctx, chatID, "Не удалось сохранить завершение формы. Текущие данные оставлены, попробуй ещё раз.")
			return false
		}
	}
	b.pendingMu.Lock()
	delete(b.pending, userID)
	b.pendingMu.Unlock()
	return true
}
func (b *Bot) addTarget(ctx context.Context, chatID, userID int64, name, host string) {
	if err := b.service.AddTarget(ctx, userID, name, host); err != nil {
		b.send(ctx, chatID, "Не удалось добавить сервер: "+err.Error())
		return
	}
	b.send(ctx, chatID, fmt.Sprintf("Сохранил «%s» — %s. Открой «Мои серверы», чтобы проверить его или настроить алерт.", name, host))
}
func (b *Bot) resolveTarget(userID int64, input string) string {
	if target, ok := b.service.FindTarget(userID, input); ok {
		return target.Host
	}
	if checker.ValidHost(input) {
		return input
	}
	return ""
}
func (b *Bot) listTargets(ctx context.Context, chatID, userID int64) {
	targets := b.service.Targets(userID)
	if len(targets) == 0 {
		b.send(ctx, chatID, "Сохранённых серверов пока нет. Нажми «Добавить сервер».")
		return
	}
	keyboard := make([][]InlineButton, 0, len(targets)*2)
	var text strings.Builder
	text.WriteString("Твои серверы — выбери действие:\n")
	for i, target := range targets {
		fmt.Fprintf(&text, "\n%s — %s", target.Name, target.Host)
		keyboard = append(keyboard, []InlineButton{{Text: "🔎 " + target.Name, CallbackData: fmt.Sprintf("target-check:%d", i)}, {Text: "🚫", CallbackData: fmt.Sprintf("target-block:%d", i)}, {Text: "🔔", CallbackData: fmt.Sprintf("target-alert:%d", i)}, {Text: "🗑", CallbackData: fmt.Sprintf("target-remove:%d", i)}})
	}
	b.sendInline(ctx, chatID, text.String(), keyboard)
}
func (b *Bot) chooseAlertTarget(ctx context.Context, chatID, userID int64) {
	targets := b.service.Targets(userID)
	if len(targets) == 0 {
		b.send(ctx, chatID, "Сначала добавь сервер кнопкой «Добавить сервер».")
		return
	}
	if !b.setPending(ctx, chatID, userID, interaction{Step: "alert-target"}) {
		return
	}
	keyboard := make([][]InlineButton, 0, len(targets))
	for i, target := range targets {
		keyboard = append(keyboard, []InlineButton{{Text: target.Name + " — " + target.Host, CallbackData: fmt.Sprintf("alert-target:%d", i)}})
	}
	b.sendInline(ctx, chatID, "Для какого сервера настроить алерт?", keyboard)
}
func (b *Bot) manualCheckCommand(ctx context.Context, chatID, userID int64, args []string) {
	if len(args) < 1 || len(args) > 3 {
		b.send(ctx, chatID, "Использование: /check <сервер: IP, домен или имя> [icmp|tcp|http] [порт/URL]")
		return
	}
	target := b.resolveTarget(userID, args[0])
	if target == "" {
		b.send(ctx, chatID, "Укажи IP, домен или имя сохранённого сервера.")
		return
	}
	method, argument := "icmp", ""
	if len(args) > 1 {
		method = args[1]
	}
	if len(args) > 2 {
		argument = args[2]
	}
	b.runManualCheck(ctx, chatID, target, method, argument)
}
func (b *Bot) runManualCheck(ctx context.Context, chatID int64, host, method, argument string) {
	result, err := b.service.Check(ctx, host, method, argument)
	if err != nil {
		b.send(ctx, chatID, err.Error())
		return
	}
	status := "недоступен"
	if result.Up {
		status = "доступен"
	}
	b.send(ctx, chatID, fmt.Sprintf("%s: %s. %s", host, status, result.Detail))
}
func (b *Bot) manualBlockCommand(ctx context.Context, chatID, userID int64, args []string) {
	if len(args) != 1 {
		b.send(ctx, chatID, "Использование: /block <сервер: IP, домен или имя>")
		return
	}
	target := b.resolveTarget(userID, args[0])
	if target == "" {
		b.send(ctx, chatID, "Укажи IP, домен или имя сохранённого сервера.")
		return
	}
	b.manualBlock(ctx, chatID, target)
}
func (b *Bot) manualBlock(ctx context.Context, chatID int64, target string) {
	b.send(ctx, chatID, "Проверяю через Cheburcheck: "+target+"...")
	checkCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	result, err := b.service.Block(checkCtx, target)
	if err != nil {
		b.send(ctx, chatID, "Проверка не завершилась: "+err.Error())
		return
	}
	b.send(ctx, chatID, fmt.Sprintf("Cheburcheck для %s: %s", target, result))
}
func (b *Bot) addAlertCommand(ctx context.Context, chatID, userID int64, args []string) {
	if len(args) < 3 || len(args) > 4 {
		b.send(ctx, chatID, "Использование: /alert <имя> <icmp|tcp|http> <интервал> [порт/URL]")
		return
	}
	interval, err := time.ParseDuration(args[2])
	if err != nil || interval <= 0 {
		b.send(ctx, chatID, "Интервал должен быть положительным, например 30s, 5m или 2h.")
		return
	}
	alert, err := b.service.AddAlert(ctx, chatID, userID, args[0], args[1], at(args, 3), interval)
	if err != nil {
		b.send(ctx, chatID, err.Error())
		return
	}
	b.send(ctx, chatID, fmt.Sprintf("Алерт настроен для «%s»: %s, каждые %s. При недоступности проверю Cheburcheck.", alert.Target.Name, alert.Method, alert.Interval))
}
func (b *Bot) listAlerts(ctx context.Context, chatID, userID int64) {
	alerts := b.service.Alerts(userID)
	if len(alerts) == 0 {
		b.send(ctx, chatID, "Алертов пока нет. Нажми «Настроить алерт».")
		return
	}
	keyboard := make([][]InlineButton, 0, len(alerts))
	var text strings.Builder
	text.WriteString("Твои алерты:\n")
	for _, alert := range alerts {
		state := "ожидает проверки"
		if alert.LastUp != nil {
			if *alert.LastUp {
				state = "доступен"
			} else {
				state = "недоступен"
			}
		}
		fmt.Fprintf(&text, "\n%s — %s, %s, каждые %s; %s", alert.Target.Name, alert.Method, alert.Argument, alert.Interval, state)
		keyboard = append(keyboard, []InlineButton{{Text: "Удалить «" + alert.Target.Name + "»", CallbackData: "alert-remove:" + alert.ID}})
	}
	b.sendInline(ctx, chatID, text.String(), keyboard)
}

func at(values []string, index int) string {
	if index >= len(values) {
		return ""
	}
	return values[index]
}

var _ monitor.Notifier = (*API)(nil)
