package telegram

import (
	"encoding/json"
	"fmt"
	"html"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/theillarion/tg-net-bot/internal/blockcheck"
)

func formatBlockResult(result blockcheck.Result) string {
	var out strings.Builder
	fmt.Fprintf(&out, "<b>Проверка блокировок · %s</b>\n", html.EscapeString(result.Target))
	state := "не найдено в базовых списках"
	if result.Blocked {
		state = "найдено в базовых списках"
	}
	fmt.Fprintf(&out, "База: <b>%s</b>\nТип цели: %s\n", state, html.EscapeString(emptyAsUnknown(result.TargetType)))
	if result.RKNDomain != nil {
		fmt.Fprintf(&out, "Домен РКН: %s\n", html.EscapeString(*result.RKNDomain))
	} else {
		out.WriteString("Домен РКН: не указан\n")
	}
	fmt.Fprintf(&out, "IP: %s\n", html.EscapeString(joinOrUnknown(result.IPs)))
	fmt.Fprintf(&out, "Подсети в списках: %s\n", html.EscapeString(joinOrUnknown(result.BlockedSubnets)))
	fmt.Fprintf(&out, "CDN/провайдеры: %s\n", html.EscapeString(formatProviders(result.CDNProviders)))
	fmt.Fprintf(&out, "Geo: %s\n", html.EscapeString(rawOrUnknown(result.Geo)))
	fmt.Fprintf(&out, "ASN/организация: %s\n", html.EscapeString(rawOrUnknown(result.ASNInfo)))
	fmt.Fprintf(&out, "Whitelist: %s\n", html.EscapeString(rawOrUnknown(result.Whitelist)))
	fmt.Fprintf(&out, "Размер подсети: %s\n", html.EscapeString(pointerOrUnknown(result.SubnetSize)))
	fmt.Fprintf(&out, "Reverse DNS: %s\n", html.EscapeString(joinOrUnknown(result.ReverseLookup)))
	fmt.Fprintf(&out, "Жалобы: %s\n", html.EscapeString(rawListOrUnknown(result.Complaints)))
	if result.ProbeAvailable && result.CacheKey != "" {
		out.WriteString("\nРасширенный probe не запускался — это отдельный запрос с лимитом 5/мин (default Cheburcheck; deployment может отличаться). Контекст probe хранится только до допустимого срока действия ID.")
	} else {
		out.WriteString("\nID расширенного probe недоступен.")
	}
	return truncateTelegram(out.String(), 3900)
}

func formatProbeResult(result blockcheck.ProbeResult) string {
	var out strings.Builder
	fmt.Fprintf(&out, "<b>Расширенный probe · %s</b>\n", html.EscapeString(emptyAsUnknown(result.Target)))
	fmt.Fprintf(&out, "Узлов онлайн: %d\nОтветили: %d\nБез ответа: %d\n",
		result.Stats.Online, result.Stats.Responded, result.Stats.Unanswered)
	if len(result.Verdicts) == 0 {
		out.WriteString("Категории вердиктов: источник не предоставил.\n")
	} else {
		labels := make([]string, 0, len(result.Verdicts))
		for label := range result.Verdicts {
			labels = append(labels, label)
		}
		sort.Strings(labels)
		out.WriteString("Вердикты (точные метки источника): ")
		for i, label := range labels {
			if i > 0 {
				out.WriteString(", ")
			}
			fmt.Fprintf(&out, "%s: %d", html.EscapeString(label), result.Verdicts[label])
		}
		out.WriteByte('\n')
	}
	for i, event := range result.Events {
		if i >= 8 {
			fmt.Fprintf(&out, "… и ещё %d событий", len(result.Events)-i)
			break
		}
		pretty, err := json.MarshalIndent(json.RawMessage(event.Data), "", "  ")
		if err != nil {
			continue
		}
		fmt.Fprintf(&out, "\n<b>%s</b>\n<pre>%s</pre>", html.EscapeString(event.Type), html.EscapeString(string(pretty)))
	}
	return truncateTelegram(out.String(), 3900)
}

func formatProbeProgress(result blockcheck.ProbeResult) string {
	var out strings.Builder
	fmt.Fprintf(&out, "<b>Расширенный probe · %s</b>\n", html.EscapeString(emptyAsUnknown(result.Target)))
	fmt.Fprintf(&out, "Статус: получаю ответы узлов…\nОнлайн: %d · Ответили: %d · Без ответа: %d\n",
		result.Stats.Online, result.Stats.Responded, result.Stats.Unanswered)
	if len(result.Verdicts) > 0 {
		labels := make([]string, 0, len(result.Verdicts))
		for label := range result.Verdicts {
			labels = append(labels, label)
		}
		sort.Strings(labels)
		out.WriteString("Вердикты источника: ")
		for i, label := range labels {
			if i > 0 {
				out.WriteString(", ")
			}
			fmt.Fprintf(&out, "%s: %d", html.EscapeString(label), result.Verdicts[label])
		}
		out.WriteByte('\n')
	}
	return out.String()
}

func blockResultsKeyboard(results []operationResult) [][]InlineButton {
	rows := make([][]InlineButton, 0, len(results)+1)
	for _, result := range results {
		if result.cacheKey != "" {
			rows = append(rows, []InlineButton{{Text: "🛰 Probe · " + result.target.Name, CallbackData: "block-probe:" + result.cacheKey}})
		}
	}
	return withNavigation(rows, "menu:services")
}

func formatOperationResults(action string, results []operationResult, done, total int) string {
	var out strings.Builder
	out.WriteString("<b>Результаты проверки блокировки</b>\n")
	fmt.Fprintf(&out, "Готово: %d / %d\n\n", done, total)
	for _, result := range results {
		fmt.Fprintf(&out, "<b>%s</b> · %s\n%s\n\n",
			html.EscapeString(result.target.Name), html.EscapeString(result.status), result.detail)
	}
	out.WriteString("<i>Лимиты Cheburcheck по текущему upstream default: 30 check/мин и 5 probe/мин, burst 1. Развёртывание сервиса может отличаться. В batch probe автоматически не запускается.</i>")
	return truncateTelegram(out.String(), 3900)
}

func formatProviders(providers map[string][]json.RawMessage) string {
	if len(providers) == 0 {
		return "нет данных"
	}
	names := make([]string, 0, len(providers))
	for name := range providers {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("%s (%d)", name, len(providers[name])))
	}
	return strings.Join(parts, ", ")
}

func rawOrUnknown(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return "нет данных"
	}
	return string(raw)
}

func rawListOrUnknown(values []json.RawMessage) string {
	if len(values) == 0 {
		return "нет данных"
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, string(value))
	}
	return strings.Join(parts, ", ")
}

func joinOrUnknown(values []string) string {
	if len(values) == 0 {
		return "нет данных"
	}
	return strings.Join(values, ", ")
}

func pointerOrUnknown(value *string) string {
	if value == nil || strings.TrimSpace(*value) == "" {
		return "нет данных"
	}
	return *value
}

func emptyAsUnknown(value string) string {
	if strings.TrimSpace(value) == "" {
		return "не указан"
	}
	return value
}

func truncateTelegram(value string, max int) string {
	var out strings.Builder
	stack := make([]string, 0, 4)
	visible := 0
	truncated := false
	for i := 0; i < len(value); {
		if value[i] == '<' {
			end := strings.IndexByte(value[i:], '>')
			if end >= 0 {
				end += i
				tag := value[i : end+1]
				name := strings.Trim(strings.TrimPrefix(strings.TrimSuffix(tag, ">"), "<"), "/")
				if strings.HasPrefix(tag, "</") {
					for j := len(stack) - 1; j >= 0; j-- {
						if stack[j] == name {
							stack = stack[:j]
							break
						}
					}
				} else if name == "b" || name == "i" || name == "pre" {
					stack = append(stack, name)
				}
				out.WriteString(tag)
				i = end + 1
				continue
			}
		}
		if value[i] == '&' {
			end := strings.IndexByte(value[i:], ';')
			if end > 0 && end <= 10 {
				entity := value[i : i+end+1]
				if visible+1 > max {
					truncated = true
					break
				}
				out.WriteString(entity)
				visible++
				i += end + 1
				continue
			}
		}
		r, size := utf8.DecodeRuneInString(value[i:])
		if visible+1 > max {
			truncated = true
			break
		}
		out.WriteRune(r)
		visible++
		i += size
	}
	if truncated {
		out.WriteRune('…')
		for j := len(stack) - 1; j >= 0; j-- {
			fmt.Fprintf(&out, "</%s>", stack[j])
		}
	}
	return out.String()
}
