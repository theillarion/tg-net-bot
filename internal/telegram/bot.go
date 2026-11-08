package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
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
	buttonCheck    = "🔎 Проверить доступность"
	buttonBlock    = "🚫 Проверить блокировку"
	buttonAdd      = "➕ Добавить сервер"
	buttonTargets  = "🖥 Мои серверы"
	buttonNewRule  = "🔔 Настроить алерт"
	buttonRules    = "📋 Мои алерты"
	buttonHelp     = "❓ Помощь"
	buttonServices = "🧰 Сервисы"
	buttonServers  = "🖥 Серверы"
	buttonAlerts   = "🔔 Алерты"
)

type interaction = domain.Interaction

type InteractionStore interface {
	LoadInteractions(context.Context) (map[int64]domain.Interaction, error)
	SaveInteraction(context.Context, int64, domain.Interaction) error
	DeleteInteraction(context.Context, int64) error
}

type Bot struct {
	pendingMu       sync.Mutex
	queuesMu        sync.Mutex
	api             *API
	service         *monitor.Service
	allowed         map[int64]struct{}
	store           InteractionStore
	pending         map[int64]interaction
	queues          map[int64]chan Update
	logger          *slog.Logger
	uiMu            sync.Mutex
	screens         map[int64]int
	screenLocks     map[int64]*sync.Mutex
	targetSnapshots map[int64][]domain.Target
	operations      map[int64]*activeOperation
	workSlots       chan struct{}
}

type activeOperation struct {
	cancel context.CancelFunc
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
	MessageID int    `json:"message_id"`
	Text      string `json:"text"`
	Chat      Chat   `json:"chat"`
	From      User   `json:"from"`
}
type CallbackQuery struct {
	ID      string   `json:"id"`
	From    User     `json:"from"`
	Message *Message `json:"message"`
	Data    string   `json:"data"`
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
	return &Bot{api: api, service: service, allowed: allowed, store: store, pending: make(map[int64]interaction), queues: make(map[int64]chan Update), logger: logger, screens: make(map[int64]int), screenLocks: make(map[int64]*sync.Mutex), targetSnapshots: make(map[int64][]domain.Target), operations: make(map[int64]*activeOperation), workSlots: make(chan struct{}, 4)}
}

func (b *Bot) Run(ctx context.Context) error {
	if b.store != nil {
		pending, err := b.store.LoadInteractions(ctx)
		if err != nil {
			return fmt.Errorf("restore Telegram dialog state: %w", err)
		}
		for userID, state := range pending {
			if isLegacyAlertInteraction(state) {
				state = interaction{Step: "alert-stale"}
				if err := b.store.SaveInteraction(ctx, userID, state); err != nil {
					return fmt.Errorf("reset legacy Telegram alert form: %w", err)
				}
				pending[userID] = state
			}
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
	case "start":
		if !b.resetPending(ctx, chatID, userID) {
			return
		}
		b.sendMenu(ctx, chatID)
	case "help":
		if !b.resetPending(ctx, chatID, userID) {
			return
		}
		b.cancelOperation(chatID)
		b.sendInline(ctx, chatID, helpText, nil)
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
	case buttonServices, buttonServers, buttonAlerts, buttonCheck, buttonBlock, buttonAdd, buttonTargets, buttonNewRule, buttonRules, buttonHelp:
	default:
		return false
	}
	switch input {
	case buttonServices:
		if !b.resetPending(ctx, chatID, userID) {
			return true
		}
		b.showServices(ctx, chatID)
	case buttonServers:
		if !b.resetPending(ctx, chatID, userID) {
			return true
		}
		b.listTargets(ctx, chatID, userID)
	case buttonAlerts:
		if !b.resetPending(ctx, chatID, userID) {
			return true
		}
		b.listAlerts(ctx, chatID, userID)
	case buttonCheck:
		b.startTargetPicker(ctx, chatID, userID, "check")
	case buttonBlock:
		b.startTargetPicker(ctx, chatID, userID, "block")
	case buttonAdd:
		if !b.setPending(ctx, chatID, userID, interaction{Step: "add-name"}) {
			return true
		}
		b.send(ctx, chatID, "Как назвать сервер?")
	case buttonTargets:
		if !b.resetPending(ctx, chatID, userID) {
			return true
		}
		b.listTargets(ctx, chatID, userID)
	case buttonNewRule:
		b.chooseAlertTarget(ctx, chatID, userID)
	case buttonRules:
		if !b.resetPending(ctx, chatID, userID) {
			return true
		}
		b.listAlerts(ctx, chatID, userID)
	case buttonHelp:
		if !b.resetPending(ctx, chatID, userID) {
			return true
		}
		b.cancelOperation(chatID)
		b.sendInline(ctx, chatID, helpText, nil)
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
	case "check-host":
		target := b.resolveTarget(userID, value)
		if target == "" {
			b.send(ctx, chatID, "Не распознал IP, домен или имя сервера. Попробуй ещё раз.")
			return
		}
		if !b.setPending(ctx, chatID, userID, interaction{Step: "check-method", Host: target}) {
			return
		}
		b.sendInline(ctx, chatID, "Каким способом проверить доступность?", methodKeyboard("check-method"))
	case "block-host":
		target := b.resolveTarget(userID, value)
		if target == "" {
			b.send(ctx, chatID, "Не распознал IP, домен или имя сервера. Попробуй ещё раз.")
			return
		}
		b.startOperation(ctx, chatID, "block", []domain.Target{{Name: value, Host: target}}, "", "")
		b.clearPending(ctx, chatID, userID)
	case "manual-tcp-port":
		if _, _, err := checker.Parse("tcp", value); err != nil {
			b.send(ctx, chatID, err.Error()+" Отправь порт от 1 до 65535.")
			return
		}
		b.startOperation(ctx, chatID, "check", []domain.Target{{Name: pending.Host, Host: pending.Host}}, "tcp", value)
		b.clearPending(ctx, chatID, userID)
	case "manual-tcp-port-batch":
		if _, _, err := checker.Parse("tcp", value); err != nil {
			b.send(ctx, chatID, err.Error()+" Отправь порт от 1 до 65535.")
			return
		}
		pending.Method, pending.Argument = "tcp", value
		if !b.setPending(ctx, chatID, userID, pending) {
			return
		}
		b.runPickedTargets(ctx, chatID, userID)
	case "block-target":
		target := b.resolveTarget(userID, value)
		if target == "" {
			b.send(ctx, chatID, "Не нашёл IP, домен или сохранённый сервер с таким именем.")
			return
		}
		b.manualBlock(ctx, chatID, target)
		b.clearPending(ctx, chatID, userID)
	case "alert-stale":
		if !b.clearPending(ctx, chatID, userID) {
			return
		}
		b.sendInline(ctx, chatID, "Предыдущая настройка алерта была начата до обновления бота и не восстановлена. Начни настройку заново.", [][]InlineButton{{{Text: "➕ Настроить алерт", CallbackData: "alert:new"}}})
	case "add-alert-interval":
		interval, err := time.ParseDuration(value)
		if err != nil || interval <= 0 {
			b.send(ctx, chatID, "Нужен положительный интервал, например 30s, 5m или 2h. Попробуй ещё раз.")
			return
		}
		pending.Name = interval.String()
		pending.Step = "alert-review"
		if !b.setPending(ctx, chatID, userID, pending) {
			return
		}
		b.renderAlertReview(ctx, chatID, userID)
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
		b.askAlertInterval(ctx, chatID, userID, pending)
	}
}

func (b *Bot) handleCallback(ctx context.Context, callback CallbackQuery) {
	chatID := int64(0)
	if callback.Message != nil {
		chatID = callback.Message.Chat.ID
		if callback.Message.MessageID != 0 {
			b.uiMu.Lock()
			b.screens[chatID] = callback.Message.MessageID
			b.uiMu.Unlock()
		}
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
	case "menu":
		if !b.resetPending(ctx, chatID, callback.From.ID) {
			return
		}
		b.cancelOperation(chatID)
		switch parts[1] {
		case "services":
			b.showServices(ctx, chatID)
		case "servers":
			b.listTargets(ctx, chatID, callback.From.ID)
		case "alerts":
			b.listAlerts(ctx, chatID, callback.From.ID)
		case "help":
			b.sendInline(ctx, chatID, helpText, nil)
		}
	case "nav":
		b.cancelOperation(chatID)
		if parts[1] == "home" {
			if !b.resetPending(ctx, chatID, callback.From.ID) {
				return
			}
			b.renderMainMenu(ctx, chatID)
		} else {
			b.navigateBack(ctx, chatID, callback.From.ID)
		}
	case "service":
		b.cancelOperation(chatID)
		switch parts[1] {
		case "check":
			b.startTargetPicker(ctx, chatID, callback.From.ID, "check")
		case "block":
			b.startTargetPicker(ctx, chatID, callback.From.ID, "block")
		case "check-host", "block-host":
			step := "check-host"
			if parts[1] == "block-host" {
				step = "block-host"
			}
			if b.setPending(ctx, chatID, callback.From.ID, interaction{Step: step}) {
				b.send(ctx, chatID, "Отправь IP, домен или имя сохранённого сервера.")
			}
		}
	case "server":
		if parts[1] == "add" {
			if b.setPending(ctx, chatID, callback.From.ID, interaction{Step: "add-name"}) {
				b.send(ctx, chatID, "Как назвать сервер?")
			}
		}
	case "alert":
		if parts[1] == "new" {
			b.chooseAlertTarget(ctx, chatID, callback.From.ID)
		}
	case "alert-back":
		pending := b.getPending(callback.From.ID)
		if pending.Step == "alert-review" {
			pending.Step = "alert-select"
			if b.setPending(ctx, chatID, callback.From.ID, pending) {
				b.showAlertPicker(ctx, chatID, callback.From.ID, pending)
			}
		}
	case "target-pick":
		b.toggleTargetSelection(ctx, chatID, callback.From.ID, at(parts, 1))
	case "target-all":
		b.toggleAllTargets(ctx, chatID, callback.From.ID)
	case "target-run":
		b.runPickedTargets(ctx, chatID, callback.From.ID)
	case "alert-pick":
		b.toggleAlertTarget(ctx, chatID, callback.From.ID, at(parts, 1))
	case "alert-all":
		b.toggleAllAlertTargets(ctx, chatID, callback.From.ID)
	case "alert-continue":
		b.continueAlertWizard(ctx, chatID, callback.From.ID)
	case "alert-review":
		b.renderAlertReview(ctx, chatID, callback.From.ID)
	case "alert-interval":
		pending := b.getPending(callback.From.ID)
		if pending.Step != "add-alert-interval" {
			b.send(ctx, chatID, "Экран интервала уже устарел.")
			return
		}
		pending.Name = parts[1]
		pending.Step = "alert-review"
		if b.setPending(ctx, chatID, callback.From.ID, pending) {
			b.renderAlertReview(ctx, chatID, callback.From.ID)
		}
	case "check-method":
		b.selectManualMethod(ctx, chatID, callback.From.ID, parts[1])
	case "manual-http":
		pending := b.getPending(callback.From.ID)
		if pending.Step == "manual-http-batch" {
			pending.Method, pending.Argument = "http", parts[1]
			if b.setPending(ctx, chatID, callback.From.ID, pending) {
				b.runPickedTargets(ctx, chatID, callback.From.ID)
			}
			return
		}
		if pending.Step != "manual-http" || !checker.ValidHost(pending.Host) {
			b.send(ctx, chatID, "Это меню проверки уже устарело. Начни проверку заново кнопкой «Проверить доступность».")
			return
		}
		b.startOperation(ctx, chatID, "check", []domain.Target{{Name: pending.Host, Host: pending.Host}}, "http", parts[1])
		b.clearPending(ctx, chatID, callback.From.ID)
	case "alert-target":
		b.selectAlertTarget(ctx, chatID, callback.From.ID, parts[1])
	case "alert-method":
		b.selectAlertMethod(ctx, chatID, callback.From.ID, parts[1])
	case "alert-http":
		pending := b.getPending(callback.From.ID)
		if pending.Step != "alert-http" || len(decodeNames(pending.RequestID)) == 0 {
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
	case "alert-view":
		b.showAlertDetail(ctx, chatID, callback.From.ID, at(parts, 1))
	}
}

func (b *Bot) selectManualMethod(ctx context.Context, chatID, userID int64, method string) {
	pending := b.getPending(userID)
	batch := pending.Step == "check-method-batch"
	if (!batch && (pending.Step != "check-method" || !checker.ValidHost(pending.Host))) || batch && len(decodeNames(pending.RequestID)) == 0 {
		b.send(ctx, chatID, "Это меню проверки уже устарело. Начни проверку заново кнопкой «Проверить доступность».")
		return
	}
	switch method {
	case "icmp":
		if batch {
			pending.Method = method
			if b.setPending(ctx, chatID, userID, pending) {
				b.runPickedTargets(ctx, chatID, userID)
			}
		} else {
			b.startOperation(ctx, chatID, "check", []domain.Target{{Name: pending.Host, Host: pending.Host}}, method, "")
			b.clearPending(ctx, chatID, userID)
		}
	case "tcp":
		pending.Step = "manual-tcp-port"
		if batch {
			pending.Step = "manual-tcp-port-batch"
		}
		if !b.setPending(ctx, chatID, userID, pending) {
			return
		}
		b.send(ctx, chatID, "Какой TCP-порт проверить? Отправь номер, например 443.")
	case "http":
		pending.Step = "manual-http"
		if batch {
			pending.Step = "manual-http-batch"
		}
		if !b.setPending(ctx, chatID, userID, pending) {
			return
		}
		b.sendInline(ctx, chatID, "Проверить HTTPS или HTTP?", [][]InlineButton{{{Text: "HTTPS", CallbackData: "manual-http:https"}, {Text: "HTTP", CallbackData: "manual-http:http"}}})
	default:
		b.send(ctx, chatID, "Неизвестный способ проверки. Начни проверку заново.")
	}
}

func (b *Bot) selectAlertTarget(ctx context.Context, chatID, userID int64, rawIndex string) {
	pending := b.getPending(userID)
	if pending.Step != "alert-select" {
		b.send(ctx, chatID, "Это меню выбора серверов устарело. Открой настройку алерта заново.")
		return
	}
	index, err := strconv.Atoi(rawIndex)
	candidates := decodeNames(pending.Host)
	if err != nil || index < 0 || index >= len(candidates) {
		b.send(ctx, chatID, "Не получилось выбрать сервер. Открой настройку алерта ещё раз.")
		return
	}
	pending.RequestID = toggleName(pending.RequestID, candidates[index])
	if b.setPending(ctx, chatID, userID, pending) {
		b.showAlertPicker(ctx, chatID, userID, pending)
	}
}

func (b *Bot) selectAlertMethod(ctx context.Context, chatID, userID int64, method string) {
	pending := b.getPending(userID)
	selected := decodeNames(pending.RequestID)
	if pending.Step != "alert-method" || len(selected) == 0 {
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
		b.sendInline(ctx, chatID, "Какой TCP-порт проверять? Отправь номер, например 443.", nil)
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
	b.sendInline(ctx, chatID, "Выбери интервал или отправь свой, например 1m, 15m или 2h.", [][]InlineButton{
		{{Text: "1 мин", CallbackData: "alert-interval:1m"}, {Text: "5 мин", CallbackData: "alert-interval:5m"}},
		{{Text: "15 мин", CallbackData: "alert-interval:15m"}, {Text: "1 час", CallbackData: "alert-interval:1h"}},
	})
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
	b.uiMu.Lock()
	targets := append([]domain.Target(nil), b.targetSnapshots[chatID]...)
	b.uiMu.Unlock()
	if targets == nil {
		targets = b.service.Targets(userID)
	}
	if err != nil || index < 0 || index >= len(targets) {
		b.send(ctx, chatID, "Список серверов изменился. Открой «Мои серверы» ещё раз.")
		return
	}
	target := targets[index]
	if current, ok := b.service.FindTarget(userID, target.Name); !ok || current != target {
		b.send(ctx, chatID, "Список серверов изменился. Открой его ещё раз.")
		return
	}
	switch action {
	case "target-check":
		if !b.setPending(ctx, chatID, userID, interaction{Step: "check-method", Host: target.Host}) {
			return
		}
		b.sendInline(ctx, chatID, fmt.Sprintf("Как проверить «%s»?", html.EscapeString(target.Name)), methodKeyboard("check-method"))
	case "target-block":
		b.startOperation(ctx, chatID, "block", []domain.Target{target}, "", "")
	case "target-alert":
		names := make([]string, 0, len(targets))
		for _, item := range targets {
			names = append(names, item.Name)
		}
		pending := interaction{Step: "alert-select", Host: encodeNames(names), RequestID: encodeNames([]string{target.Name})}
		if b.setPending(ctx, chatID, userID, pending) {
			b.showAlertPicker(ctx, chatID, userID, pending)
		}
	case "target-remove":
		if err := b.service.RemoveTarget(ctx, userID, target.Name); err != nil {
			b.send(ctx, chatID, "Не удалось удалить сервер: "+err.Error())
			return
		}
		b.listTargets(ctx, chatID, userID)
	}
}

const helpText = "<b>Разделы бота</b>\n\n🧰 Сервисы — разовые проверки доступности и блокировки для выбранных серверов или введённого адреса.\n🖥 Серверы — список, добавление, проверка и удаление серверов.\n🔔 Алерты — просмотр и настройка правил мониторинга. Мастер настройки показывает выбранные серверы и параметры; сохранение пакетного алерта для нескольких серверов отложено до согласования правил.\n\nДля ручной проверки можно ввести IPv4, IPv6 или домен. Команды /check, /block, /add и /alert тоже доступны. Группы серверов пока не включены в меню и будут добавлены после согласования правил группировки."

func (b *Bot) send(ctx context.Context, chatID int64, text string) {
	b.render(ctx, chatID, html.EscapeString(text), navigationKeyboard("nav:back"))
}
func (b *Bot) sendMenu(ctx context.Context, chatID int64) {
	b.cancelOperation(chatID)
	lock := b.screenLock(chatID)
	lock.Lock()
	defer lock.Unlock()
	if err := ctx.Err(); err != nil {
		return
	}
	b.uiMu.Lock()
	messageID := b.screens[chatID]
	b.uiMu.Unlock()
	if messageID != 0 {
		b.renderLocked(ctx, chatID, "<b>Панель управления</b>\n\nВыбери раздел.", mainMenuKeyboard())
		return
	}
	messageID, err := b.api.SendScreen(ctx, chatID, "<b>Панель управления</b>\n\nВыбери раздел.", map[string]any{"inline_keyboard": mainMenuKeyboard()})
	if err != nil {
		b.logger.Error("send Telegram menu", "chat_id", chatID, "error", err)
		return
	}
	b.uiMu.Lock()
	b.screens[chatID] = messageID
	b.uiMu.Unlock()
}
func (b *Bot) sendInline(ctx context.Context, chatID int64, text string, keyboard [][]InlineButton) {
	b.render(ctx, chatID, text, withNavigation(keyboard, "nav:back"))
}

func (b *Bot) render(ctx context.Context, chatID int64, text string, keyboard [][]InlineButton) {
	if ctx.Err() != nil {
		return
	}
	lock := b.screenLock(chatID)
	lock.Lock()
	defer lock.Unlock()
	if ctx.Err() != nil {
		return
	}
	b.renderLocked(ctx, chatID, text, keyboard)
}

func (b *Bot) screenLock(chatID int64) *sync.Mutex {
	b.uiMu.Lock()
	defer b.uiMu.Unlock()
	if b.screenLocks == nil {
		b.screenLocks = make(map[int64]*sync.Mutex)
	}
	lock := b.screenLocks[chatID]
	if lock == nil {
		lock = &sync.Mutex{}
		b.screenLocks[chatID] = lock
	}
	return lock
}

func (b *Bot) renderLocked(ctx context.Context, chatID int64, text string, keyboard [][]InlineButton) {
	if ctx.Err() != nil {
		return
	}
	markup := map[string]any{"inline_keyboard": keyboard}
	b.uiMu.Lock()
	messageID := b.screens[chatID]
	b.uiMu.Unlock()
	if messageID != 0 {
		if err := b.api.EditScreen(ctx, chatID, messageID, text, markup); err == nil {
			return
		} else if strings.Contains(strings.ToLower(err.Error()), "message is not modified") {
			return
		} else {
			b.logger.Warn("edit Telegram screen failed; sending replacement", "chat_id", chatID, "error", err)
		}
	}
	if ctx.Err() != nil {
		return
	}
	newID, err := b.api.SendScreen(ctx, chatID, text, markup)
	if err != nil {
		b.logger.Error("send Telegram screen", "chat_id", chatID, "error", err)
		return
	}
	b.uiMu.Lock()
	b.screens[chatID] = newID
	b.uiMu.Unlock()
}

func withNavigation(keyboard [][]InlineButton, back string) [][]InlineButton {
	result := make([][]InlineButton, len(keyboard))
	for i := range keyboard {
		result[i] = append([]InlineButton(nil), keyboard[i]...)
	}
	result = append(result, []InlineButton{{Text: "🔙 Назад", CallbackData: back}, {Text: "🏠 Главное меню", CallbackData: "nav:home"}})
	return result
}

func navigationKeyboard(back string) [][]InlineButton { return withNavigation(nil, back) }

func (b *Bot) renderMainMenu(ctx context.Context, chatID int64) {
	b.cancelOperation(chatID)
	b.render(ctx, chatID, "<b>Панель управления</b>\n\nВыбери раздел.", mainMenuKeyboard())
}

func mainMenuKeyboard() [][]InlineButton {
	return [][]InlineButton{
		{{Text: "🧰 Сервисы", CallbackData: "menu:services"}},
		{{Text: "🖥 Серверы", CallbackData: "menu:servers"}, {Text: "🔔 Алерты", CallbackData: "menu:alerts"}},
		{{Text: "❓ Помощь", CallbackData: "menu:help"}},
	}
}

func (b *Bot) showServices(ctx context.Context, chatID int64) {
	b.cancelOperation(chatID)
	keyboard := [][]InlineButton{
		{{Text: "🔎 Проверить доступность", CallbackData: "service:check"}},
		{{Text: "🚫 Проверить блокировку", CallbackData: "service:block"}},
		{{Text: "🌐 Проверить адрес вручную", CallbackData: "service:check-host"}},
		{{Text: "🚫 Проверить адрес вручную", CallbackData: "service:block-host"}},
	}
	b.render(ctx, chatID, "<b>Сервисы</b>\n\nВыбери сетевую проверку.", withNavigation(keyboard, "nav:home"))
}

func (b *Bot) navigateBack(ctx context.Context, chatID, userID int64) {
	b.cancelOperation(chatID)
	pending := b.getPending(userID)
	switch pending.Step {
	case "alert-select":
		if !b.clearPending(ctx, chatID, userID) {
			return
		}
		b.listAlerts(ctx, chatID, userID)
	case "alert-review":
		pending.Step = "add-alert-interval"
		if b.setPending(ctx, chatID, userID, pending) {
			b.askAlertInterval(ctx, chatID, userID, pending)
		}
	case "add-alert-interval", "alert-http", "add-alert-tcp-port":
		pending.Step = "alert-method"
		if b.setPending(ctx, chatID, userID, pending) {
			b.sendInline(ctx, chatID, "Выбери метод проверки.", methodKeyboard("alert-method"))
		}
	case "alert-method":
		pending.Step = "alert-select"
		if b.setPending(ctx, chatID, userID, pending) {
			b.showAlertPicker(ctx, chatID, userID, pending)
		}
	case "check-select", "block-select":
		if !b.clearPending(ctx, chatID, userID) {
			return
		}
		b.showServices(ctx, chatID)
	case "check-method-batch", "manual-tcp-port-batch", "manual-http-batch":
		pending.Step = "check-select"
		if b.setPending(ctx, chatID, userID, pending) {
			b.showTargetPicker(ctx, chatID, userID, pending)
		}
	case "check-method", "manual-http", "manual-tcp-port":
		if !b.clearPending(ctx, chatID, userID) {
			return
		}
		b.showServices(ctx, chatID)
	case "add-name", "add-host":
		if !b.clearPending(ctx, chatID, userID) {
			return
		}
		b.listTargets(ctx, chatID, userID)
	default:
		if !b.resetPending(ctx, chatID, userID) {
			return
		}
		b.renderMainMenu(ctx, chatID)
	}
}

func (b *Bot) cancelOperation(chatID int64) {
	b.uiMu.Lock()
	operation := b.operations[chatID]
	delete(b.operations, chatID)
	b.uiMu.Unlock()
	if operation != nil {
		operation.cancel()
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

func (b *Bot) resetPending(ctx context.Context, chatID, userID int64) bool {
	if b.getPending(userID).Step == "" {
		return true
	}
	return b.clearPending(ctx, chatID, userID)
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

func isLegacyAlertInteraction(state interaction) bool {
	switch state.Step {
	case "alert-target":
		return true
	case "alert-method", "alert-http", "add-alert-tcp-port", "add-alert-interval":
		var selected []string
		return json.Unmarshal([]byte(state.RequestID), &selected) != nil || len(selected) == 0
	default:
		return false
	}
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
	b.cancelOperation(chatID)
	targets := b.service.Targets(userID)
	if len(targets) == 0 {
		b.uiMu.Lock()
		b.targetSnapshots[chatID] = nil
		b.uiMu.Unlock()
		keyboard := [][]InlineButton{{{Text: "➕ Добавить сервер", CallbackData: "server:add"}}}
		b.render(ctx, chatID, "<b>Серверы</b>\n\nПока нет сохранённых серверов.", withNavigation(keyboard, "nav:home"))
		return
	}
	keyboard := make([][]InlineButton, 0, len(targets)*2+1)
	b.uiMu.Lock()
	b.targetSnapshots[chatID] = append([]domain.Target(nil), targets...)
	b.uiMu.Unlock()
	var text strings.Builder
	text.WriteString(fmt.Sprintf("<b>Серверы</b> · %d\n", len(targets)))
	for i, target := range targets {
		fmt.Fprintf(&text, "\n\n<b>%d. %s</b>\n<code>%s</code>", i+1, html.EscapeString(target.Name), html.EscapeString(target.Host))
		keyboard = append(keyboard,
			[]InlineButton{{Text: "🔎 Проверить", CallbackData: fmt.Sprintf("target-check:%d", i)}, {Text: "🚫 Блокировка", CallbackData: fmt.Sprintf("target-block:%d", i)}},
			[]InlineButton{{Text: "🔔 Алерт", CallbackData: fmt.Sprintf("target-alert:%d", i)}, {Text: "🗑 Удалить", CallbackData: fmt.Sprintf("target-remove:%d", i)}},
		)
	}
	keyboard = append(keyboard, []InlineButton{{Text: "➕ Добавить сервер", CallbackData: "server:add"}})
	b.render(ctx, chatID, text.String(), withNavigation(keyboard, "nav:home"))
}
func (b *Bot) chooseAlertTarget(ctx context.Context, chatID, userID int64) {
	b.cancelOperation(chatID)
	targets := b.service.Targets(userID)
	if len(targets) == 0 {
		b.render(ctx, chatID, "<b>Новый алерт</b>\n\nСначала добавь сервер.", withNavigation([][]InlineButton{{{Text: "➕ Добавить сервер", CallbackData: "server:add"}}}, "menu:alerts"))
		return
	}
	names := make([]string, 0, len(targets))
	for _, target := range targets {
		names = append(names, target.Name)
	}
	pending := interaction{Step: "alert-select", Host: encodeNames(names), RequestID: encodeNames(nil)}
	if b.setPending(ctx, chatID, userID, pending) {
		b.showAlertPicker(ctx, chatID, userID, pending)
	}
}

func (b *Bot) startTargetPicker(ctx context.Context, chatID, userID int64, action string) {
	b.cancelOperation(chatID)
	targets := b.service.Targets(userID)
	if len(targets) == 0 {
		b.render(ctx, chatID, "<b>Выбор серверов</b>\n\nСохранённых серверов пока нет. Можно ввести адрес вручную.", withNavigation([][]InlineButton{{{Text: "✍️ Ввести адрес", CallbackData: "service:" + action + "-host"}, {Text: "➕ Добавить сервер", CallbackData: "server:add"}}}, "menu:services"))
		return
	}
	names := make([]string, 0, len(targets))
	for _, target := range targets {
		names = append(names, target.Name)
	}
	step := action + "-select"
	pending := interaction{Step: step, Host: encodeNames(names), RequestID: encodeNames(nil)}
	if b.setPending(ctx, chatID, userID, pending) {
		b.showTargetPicker(ctx, chatID, userID, pending)
	}
}

func (b *Bot) showTargetPicker(ctx context.Context, chatID, userID int64, pending interaction) {
	candidates, selected := decodeNames(pending.Host), decodeNames(pending.RequestID)
	keyboard := make([][]InlineButton, 0, len(candidates)+2)
	var text strings.Builder
	if pending.Step == "check-select" {
		text.WriteString("<b>Проверка доступности</b>\nВыбери один или несколько серверов.")
	} else {
		text.WriteString("<b>Проверка блокировки</b>\nВыбери один или несколько серверов.")
	}
	for i, name := range candidates {
		target, ok := b.service.FindTarget(userID, name)
		if !ok {
			continue
		}
		mark := "▫️"
		if contains(selected, name) {
			mark = "✅"
		}
		keyboard = append(keyboard, []InlineButton{{Text: fmt.Sprintf("%s %s · %s", mark, name, target.Host), CallbackData: fmt.Sprintf("target-pick:%d", i)}})
	}
	allText := "☑️ Выбрать все"
	if len(selected) == len(candidates) && len(candidates) > 0 {
		allText = "◻️ Снять выбор"
	}
	keyboard = append(keyboard,
		[]InlineButton{{Text: allText, CallbackData: "target-all:toggle"}},
		[]InlineButton{{Text: "✍️ Ввести адрес вручную", CallbackData: "service:" + strings.TrimSuffix(pending.Step, "-select") + "-host"}},
		[]InlineButton{{Text: "Продолжить →", CallbackData: "target-run:continue"}},
	)
	b.render(ctx, chatID, text.String(), withNavigation(keyboard, "menu:services"))
}

func (b *Bot) toggleTargetSelection(ctx context.Context, chatID, userID int64, rawIndex string) {
	pending := b.getPending(userID)
	if pending.Step != "check-select" && pending.Step != "block-select" {
		b.send(ctx, chatID, "Выбор серверов устарел. Начни проверку заново.")
		return
	}
	index, err := strconv.Atoi(rawIndex)
	candidates := decodeNames(pending.Host)
	if err != nil || index < 0 || index >= len(candidates) {
		b.send(ctx, chatID, "Список серверов изменился. Открой выбор ещё раз.")
		return
	}
	if _, ok := b.service.FindTarget(userID, candidates[index]); !ok {
		b.send(ctx, chatID, "Сервер уже удалён. Обнови список.")
		return
	}
	pending.RequestID = toggleName(pending.RequestID, candidates[index])
	if b.setPending(ctx, chatID, userID, pending) {
		b.showTargetPicker(ctx, chatID, userID, pending)
	}
}

func (b *Bot) toggleAllTargets(ctx context.Context, chatID, userID int64) {
	pending := b.getPending(userID)
	if pending.Step != "check-select" && pending.Step != "block-select" {
		b.send(ctx, chatID, "Выбор серверов устарел. Начни проверку заново.")
		return
	}
	candidates, selected := decodeNames(pending.Host), decodeNames(pending.RequestID)
	if len(candidates) > 0 && len(selected) == len(candidates) {
		pending.RequestID = encodeNames(nil)
	} else {
		pending.RequestID = encodeNames(candidates)
	}
	if b.setPending(ctx, chatID, userID, pending) {
		b.showTargetPicker(ctx, chatID, userID, pending)
	}
}

func (b *Bot) runPickedTargets(ctx context.Context, chatID, userID int64) {
	pending := b.getPending(userID)
	if pending.Step == "check-select" {
		selected := decodeNames(pending.RequestID)
		if len(selected) == 0 {
			b.send(ctx, chatID, "Выбери хотя бы один сервер.")
			return
		}
		pending.Step = "check-method-batch"
		if b.setPending(ctx, chatID, userID, pending) {
			b.sendInline(ctx, chatID, "Выбранные серверы: "+html.EscapeString(strings.Join(selected, ", "))+"\n\nКаким способом проверить доступность?", methodKeyboard("check-method"))
		}
		return
	}
	if pending.Step == "block-select" {
		selected := decodeNames(pending.RequestID)
		if len(selected) == 0 {
			b.send(ctx, chatID, "Выбери хотя бы один сервер.")
			return
		}
		b.startOperation(ctx, chatID, "block", b.targetsByName(userID, selected), "", "")
		b.clearPending(ctx, chatID, userID)
		return
	}
	if !strings.HasPrefix(pending.Step, "check-") && pending.Step != "manual-tcp-port-batch" && pending.Step != "manual-http-batch" {
		b.send(ctx, chatID, "Эта форма проверки устарела. Начни её заново.")
		return
	}
	selected := decodeNames(pending.RequestID)
	if len(selected) == 0 {
		b.send(ctx, chatID, "Не выбраны серверы для проверки.")
		return
	}
	if pending.Method == "" {
		b.send(ctx, chatID, "Сначала выбери способ проверки.")
		return
	}
	b.startOperation(ctx, chatID, "check", b.targetsByName(userID, selected), pending.Method, pending.Argument)
	b.clearPending(ctx, chatID, userID)
}

func (b *Bot) targetsByName(userID int64, names []string) []domain.Target {
	targets := make([]domain.Target, 0, len(names))
	for _, name := range names {
		if target, ok := b.service.FindTarget(userID, name); ok {
			targets = append(targets, target)
		}
	}
	return targets
}

type operationResult struct {
	target         domain.Target
	status, detail string
}

func (b *Bot) startOperation(parent context.Context, chatID int64, action string, targets []domain.Target, method, argument string) {
	if len(targets) == 0 {
		b.render(parent, chatID, "Нет доступных серверов для операции.", navigationKeyboard("menu:services"))
		return
	}
	select {
	case b.workSlots <- struct{}{}:
	default:
		b.render(parent, chatID, "Сейчас выполняются другие проверки. Попробуй немного позже.", navigationKeyboard("menu:services"))
		return
	}
	ctx, cancel := context.WithCancel(parent)
	op := &activeOperation{cancel: cancel}
	b.uiMu.Lock()
	if prior := b.operations[chatID]; prior != nil {
		prior.cancel()
	}
	b.operations[chatID] = op
	b.uiMu.Unlock()
	go func() {
		defer func() {
			<-b.workSlots
			cancel()
			b.uiMu.Lock()
			if b.operations[chatID] == op {
				delete(b.operations, chatID)
			}
			b.uiMu.Unlock()
		}()
		results := make([]operationResult, 0, len(targets))
		for i, target := range targets {
			if ctx.Err() != nil {
				return
			}
			verb := "Проверяю доступность"
			if action == "block" {
				verb = "Запрашиваю проверку блокировки"
			}
			b.render(ctx, chatID, fmt.Sprintf("<b>%s</b>\n%d / %d · %s\n\nЗапрос выполняется…", verb, i+1, len(targets), html.EscapeString(target.Name)), navigationKeyboard("nav:home"))
			if action == "block" {
				checkCtx, stop := context.WithTimeout(ctx, 90*time.Second)
				value, err := b.service.Block(checkCtx, target.Host)
				stop()
				if err != nil {
					results = append(results, operationResult{target: target, status: "ошибка", detail: err.Error()})
				} else {
					results = append(results, operationResult{target: target, status: "готово", detail: value})
				}
				continue
			}
			checkCtx, stop := context.WithTimeout(ctx, 15*time.Second)
			value, err := b.service.Check(checkCtx, target.Host, method, argument)
			stop()
			if err != nil {
				results = append(results, operationResult{target: target, status: "ошибка", detail: err.Error()})
				continue
			}
			status := "недоступен"
			if value.Up {
				status = "доступен"
			}
			results = append(results, operationResult{target: target, status: status, detail: value.Detail})
		}
		if ctx.Err() != nil {
			return
		}
		var text strings.Builder
		if action == "block" {
			text.WriteString("<b>Результаты проверки блокировки</b>\n")
		} else {
			text.WriteString("<b>Результаты проверки доступности</b>\n")
		}
		for _, result := range results {
			fmt.Fprintf(&text, "\n\n<b>%s</b> · %s\n%s", html.EscapeString(result.target.Name), html.EscapeString(result.status), html.EscapeString(result.detail))
		}
		b.render(ctx, chatID, text.String(), withNavigation(nil, "menu:services"))
	}()
}

func (b *Bot) showAlertPicker(ctx context.Context, chatID, userID int64, pending interaction) {
	candidates, selected := decodeNames(pending.Host), decodeNames(pending.RequestID)
	var text strings.Builder
	text.WriteString("<b>Новый алерт</b>\nВыбери серверы, для которых хочешь подготовить одно правило.")
	keyboard := make([][]InlineButton, 0, len(candidates)+3)
	for i, name := range candidates {
		target, ok := b.service.FindTarget(userID, name)
		if !ok {
			continue
		}
		mark := "▫️"
		if contains(selected, name) {
			mark = "✅"
		}
		keyboard = append(keyboard, []InlineButton{{Text: fmt.Sprintf("%s %s · %s", mark, name, target.Host), CallbackData: fmt.Sprintf("alert-pick:%d", i)}})
	}
	allText := "☑️ Выбрать все"
	if len(selected) == len(candidates) && len(candidates) > 0 {
		allText = "◻️ Снять выбор"
	}
	keyboard = append(keyboard, []InlineButton{{Text: allText, CallbackData: "alert-all:toggle"}}, []InlineButton{{Text: "Продолжить →", CallbackData: "alert-continue:next"}})
	text.WriteString(fmt.Sprintf("\n\nВыбрано: %d", len(selected)))
	b.render(ctx, chatID, text.String(), withNavigation(keyboard, "menu:alerts"))
}

func (b *Bot) toggleAlertTarget(ctx context.Context, chatID, userID int64, rawIndex string) {
	pending := b.getPending(userID)
	if pending.Step != "alert-select" {
		b.send(ctx, chatID, "Выбор серверов устарел. Открой настройку алерта заново.")
		return
	}
	index, err := strconv.Atoi(rawIndex)
	candidates := decodeNames(pending.Host)
	if err != nil || index < 0 || index >= len(candidates) {
		b.send(ctx, chatID, "Список серверов изменился. Обнови выбор.")
		return
	}
	if _, ok := b.service.FindTarget(userID, candidates[index]); !ok {
		b.send(ctx, chatID, "Сервер уже удалён. Обнови список.")
		return
	}
	pending.RequestID = toggleName(pending.RequestID, candidates[index])
	if b.setPending(ctx, chatID, userID, pending) {
		b.showAlertPicker(ctx, chatID, userID, pending)
	}
}

func (b *Bot) toggleAllAlertTargets(ctx context.Context, chatID, userID int64) {
	pending := b.getPending(userID)
	if pending.Step != "alert-select" {
		b.send(ctx, chatID, "Выбор серверов устарел. Открой настройку алерта заново.")
		return
	}
	candidates, selected := decodeNames(pending.Host), decodeNames(pending.RequestID)
	if len(candidates) > 0 && len(selected) == len(candidates) {
		pending.RequestID = encodeNames(nil)
	} else {
		pending.RequestID = encodeNames(candidates)
	}
	if b.setPending(ctx, chatID, userID, pending) {
		b.showAlertPicker(ctx, chatID, userID, pending)
	}
}

func (b *Bot) continueAlertWizard(ctx context.Context, chatID, userID int64) {
	pending := b.getPending(userID)
	selected := decodeNames(pending.RequestID)
	if pending.Step != "alert-select" || len(selected) == 0 {
		b.send(ctx, chatID, "Выбери хотя бы один сервер.")
		return
	}
	pending.Step = "alert-method"
	if !b.setPending(ctx, chatID, userID, pending) {
		return
	}
	b.sendInline(ctx, chatID, "Для "+html.EscapeString(strings.Join(selected, ", "))+" выбери метод проверки.", methodKeyboard("alert-method"))
}

func (b *Bot) renderAlertReview(ctx context.Context, chatID, userID int64) {
	pending := b.getPending(userID)
	if pending.Step != "alert-review" {
		b.send(ctx, chatID, "Черновик настройки алерта устарел.")
		return
	}
	selected := decodeNames(pending.RequestID)
	var text strings.Builder
	fmt.Fprintf(&text, "<b>Проверь параметры алерта</b>\n\nСерверы: %s\nМетод: %s", html.EscapeString(strings.Join(selected, ", ")), strings.ToUpper(html.EscapeString(pending.Method)))
	if pending.Argument != "" {
		fmt.Fprintf(&text, "\nПараметр: %s", html.EscapeString(pending.Argument))
	}
	fmt.Fprintf(&text, "\nИнтервал: %s\n\nНастройка показана как черновик; пакетное сохранение отложено до согласования правил.", html.EscapeString(pending.Name))
	keyboard := [][]InlineButton{{{Text: "← Изменить серверы", CallbackData: "alert-back:targets"}}}
	b.render(ctx, chatID, text.String(), withNavigation(keyboard, "menu:alerts"))
}

func encodeNames(names []string) string { data, _ := json.Marshal(names); return string(data) }
func decodeNames(raw string) []string {
	var names []string
	if json.Unmarshal([]byte(raw), &names) != nil {
		return nil
	}
	return names
}
func contains(names []string, name string) bool {
	for _, item := range names {
		if item == name {
			return true
		}
	}
	return false
}
func toggleName(raw, name string) string {
	names := decodeNames(raw)
	if contains(names, name) {
		out := make([]string, 0, len(names)-1)
		for _, item := range names {
			if item != name {
				out = append(out, item)
			}
		}
		return encodeNames(out)
	}
	return encodeNames(append(names, name))
}
func escapeHTML(value string) string { return html.EscapeString(value) }
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
	b.cancelOperation(chatID)
	alerts := b.service.Alerts(userID)
	if len(alerts) == 0 {
		keyboard := [][]InlineButton{{{Text: "➕ Настроить алерт", CallbackData: "alert:new"}}}
		b.render(ctx, chatID, "<b>Алерты</b>\n\nПока нет активных алертов.", withNavigation(keyboard, "nav:home"))
		return
	}
	keyboard := make([][]InlineButton, 0, len(alerts)*2+1)
	var text strings.Builder
	text.WriteString(fmt.Sprintf("<b>Алерты</b> · %d", len(alerts)))
	for i, alert := range alerts {
		state := "ожидает проверки"
		if alert.LastUp != nil {
			if *alert.LastUp {
				state = "доступен"
			} else {
				state = "недоступен"
			}
		}
		fmt.Fprintf(&text, "\n\n<b>%d. %s</b>\n%s %s\nИнтервал: %s\nСтатус: %s", i+1, escapeHTML(alert.Target.Name), strings.ToUpper(alert.Method), escapeHTML(alert.Argument), alert.Interval, state)
		keyboard = append(keyboard,
			[]InlineButton{{Text: "🖥 " + alert.Target.Name, CallbackData: "alert-view:" + alert.ID}, {Text: "🗑 Удалить", CallbackData: "alert-remove:" + alert.ID}},
		)
	}
	keyboard = append(keyboard, []InlineButton{{Text: "➕ Настроить алерт", CallbackData: "alert:new"}})
	b.render(ctx, chatID, text.String(), withNavigation(keyboard, "nav:home"))
}

func (b *Bot) showAlertDetail(ctx context.Context, chatID, userID int64, alertID string) {
	for _, alert := range b.service.Alerts(userID) {
		if alert.ID != alertID {
			continue
		}
		state := "Ожидает первой проверки"
		if alert.LastUp != nil {
			if *alert.LastUp {
				state = "✅ Доступен"
			} else {
				state = "🔴 Недоступен"
			}
		}
		text := fmt.Sprintf("<b>Алерт · %s</b>\n\nСервер: <code>%s</code>\nМетод: %s\nПараметр: %s\nИнтервал: %s\nСостояние: %s", html.EscapeString(alert.Target.Name), html.EscapeString(alert.Target.Host), strings.ToUpper(html.EscapeString(alert.Method)), html.EscapeString(alert.Argument), alert.Interval, html.EscapeString(state))
		keyboard := [][]InlineButton{{{Text: "🗑 Удалить алерт", CallbackData: "alert-remove:" + alert.ID}}}
		b.render(ctx, chatID, text, withNavigation(keyboard, "menu:alerts"))
		return
	}
	b.send(ctx, chatID, "Алерт уже удалён или недоступен.")
}

func at(values []string, index int) string {
	if index >= len(values) {
		return ""
	}
	return values[index]
}

var _ monitor.Notifier = (*API)(nil)
