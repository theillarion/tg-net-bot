package telegram

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/theillarion/tg-net-bot/internal/blockcheck"
	"github.com/theillarion/tg-net-bot/internal/domain"
)

func TestFormatBlockResultShowsAvailableFieldsAndEscapesSourceText(t *testing.T) {
	rkn := "<evil>"
	result := blockcheck.Result{
		Target: "host.example", TargetType: "domain", Blocked: true, RKNDomain: &rkn,
		IPs: []string{"192.0.2.4"}, BlockedSubnets: []string{"192.0.2.0/24"},
		CDNProviders: map[string][]json.RawMessage{"Cloud": {json.RawMessage("{\"name\":\"edge\"}")}},
		Geo:          json.RawMessage("{\"country\":\"RU\"}"), ASNInfo: json.RawMessage("{\"asn\":64500}"),
		Complaints: []json.RawMessage{json.RawMessage("{\"date\":\"today\"}")},
		SubnetSize: stringPtr("24"), ReverseLookup: []string{"ptr.example"}, ProbeAvailable: true, CacheKey: "key",
	}
	got := formatBlockResult(result)
	for _, want := range []string{"host.example", "найдено в базовых списках", "192.0.2.4", "Cloud (1)", "ptr.example", "today", "24", "локально 3 часа"} {
		if !strings.Contains(got, want) {
			t.Fatalf("card missing %q: %s", want, got)
		}
	}
	if strings.Contains(got, "<evil>") || !strings.Contains(got, "&lt;evil&gt;") {
		t.Fatalf("upstream HTML was not escaped: %s", got)
	}
}

func stringPtr(value string) *string { return &value }

func TestFormatProbeShowsExactVerdictsAndSourceStats(t *testing.T) {
	got := formatProbeResult(blockcheck.ProbeResult{
		Target:   "host.example",
		Stats:    blockcheck.ProbeStats{Online: 5, Responded: 4, Unanswered: 1},
		Verdicts: map[string]int{"ok": 3, "uncertain": 2, "tspu_block": 1},
		Events:   []blockcheck.ProbeEvent{{Type: "result", Data: json.RawMessage("{\"verdicts\":[\"ok\"]}")}},
	})
	for _, want := range []string{"Узлов онлайн: 5", "Ответили: 4", "Без ответа: 1", "ok: 3", "uncertain: 2", "tspu_block: 1", "result"} {
		if !strings.Contains(got, want) {
			t.Fatalf("probe card missing %q: %s", want, got)
		}
	}
}

func TestFormatProbeProgressShowsReceivedEventsAndEscapesVerdict(t *testing.T) {
	got := formatProbeProgress(blockcheck.ProbeResult{
		Target:   "edge.example",
		Stats:    blockcheck.ProbeStats{Online: 5, Responded: 2, Unanswered: 3},
		Verdicts: map[string]int{"<blocked>": 1, "ok": 2},
	})
	for _, want := range []string{"получаю ответы узлов", "Онлайн: 5", "Ответили: 2", "Без ответа: 3", "&lt;blocked&gt;: 1", "ok: 2"} {
		if !strings.Contains(got, want) {
			t.Fatalf("progress missing %q: %s", want, got)
		}
	}
}

func TestFormatProbeHandlesUnknownVerdictsAndCapsLongEventHistory(t *testing.T) {
	events := make([]blockcheck.ProbeEvent, 10)
	for i := range events {
		events[i] = blockcheck.ProbeEvent{Type: "result", Data: json.RawMessage(`{"index":1}`)}
	}
	events[0].Data = json.RawMessage("not-json")
	got := formatProbeResult(blockcheck.ProbeResult{Events: events})
	if !strings.Contains(got, "Категории вердиктов: источник не предоставил") || !strings.Contains(got, "и ещё 2 событий") {
		t.Fatalf("long or incomplete probe result = %s", got)
	}
}

func TestTruncateTelegramKeepsUTF8AndClosesMarkup(t *testing.T) {
	got := truncateTelegram("<b>"+strings.Repeat("ёж", 30)+"</b>", 10)
	if !utf8.ValidString(got) || !strings.HasSuffix(got, "</b>") || !strings.Contains(got, "…") {
		t.Fatalf("truncated html=%q", got)
	}
}

func TestBlockResultsKeyboardAddsProbeOnlyWhenAvailable(t *testing.T) {
	rows := blockResultsKeyboard([]operationResult{
		{target: domain.Target{Name: "first"}},
		{target: domain.Target{Name: "second"}, cacheKey: "short-key"},
	})
	var probeButtons []InlineButton
	for _, row := range rows {
		for _, button := range row {
			if strings.HasPrefix(button.CallbackData, "block-probe:") {
				probeButtons = append(probeButtons, button)
			}
		}
	}
	if len(probeButtons) != 1 || probeButtons[0].CallbackData != "block-probe:short-key" {
		t.Fatalf("probe buttons = %+v", probeButtons)
	}
	if rows := blockResultsKeyboard(nil); len(rows) == 0 {
		t.Fatal("navigation row missing for empty result set")
	}
}
