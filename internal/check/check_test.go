package check

import (
	"strings"
	"testing"
)

func rules(fs []Finding) string {
	var out []string
	for _, f := range fs {
		out = append(out, string(f.Severity)+":"+f.Rule)
	}
	return strings.Join(out, ",")
}

func TestRun(t *testing.T) {
	cases := []struct {
		name        string
		source, tr  string
		opt         Options
		wantStatus  Status
		wantFinding string // substring of rules(), empty = no findings
	}{
		{"clean", "Pay {amount} to {merchant}", "Pague {amount} a {merchant}", Options{}, OK, ""},
		{"placeholder order may change", "{count} items in {cart}", "No {cart} há {count} artigos", Options{}, OK, ""},
		{"missing placeholder", "Pay {amount} now", "Pague agora", Options{}, Rejected, "error:placeholders"},
		{"extra placeholder", "Pay now", "Pague {amount} agora", Options{}, Rejected, "error:placeholders"},
		{"printf placeholders", "%1$s paid %2$.2f", "%2$.2f pago por %1$s", Options{}, OK, ""},
		{"printf lost", "%d new messages", "novas mensagens", Options{}, Rejected, "error:placeholders"},
		{"mustache spacing is not a change", "Hi {{ name }}", "Olá {{name}}", Options{}, OK, ""},
		{"duplicated placeholder counted", "{name} and {name}", "{name}", Options{}, Rejected, "error:placeholders"},
		{"unbalanced icu", "{count, plural, one {# item} other {# items}}", "{count, plural, one {# artigo} other {# artigos}", Options{}, Rejected, "error:icu_braces"},
		{"icu quoted brace is literal", "Use '{' to open", "Use '{' para abrir", Options{}, OK, ""},
		{"plural without other", "{n, plural, one {# card} other {# cards}}", "{n, plural, one {# cartão} many {# cartões}}", Options{}, Rejected, "error:icu_plural"},
		{"markup changed", "Read the <b>terms</b>", "Leia os termos</b>", Options{}, Rejected, "error:markup"},
		{"markup reordered is fine", "<a>Help</a> and <b>terms</b>", "<b>termos</b> e <a>ajuda</a>", Options{}, OK, ""},
		{"hard length cap", "Pay", "Efetuar o pagamento", Options{MaxLength: 10}, Rejected, "error:length"},
		{"growth warning", "Accept card payments", "Aceite pagamentos com cartão de débito e de crédito em qualquer lugar", Options{}, Warned, "warning:length_growth"},
		{"untranslated", "Card reader", "Card reader", Options{}, Warned, "warning:untranslated"},
		{"short tokens may stay", "PDF", "PDF", Options{}, OK, ""},
		{"whitespace edge", "Total: ", "Total:", Options{}, Warned, "warning:whitespace"},
		{"empty", "Pay", "   ", Options{}, Rejected, "error:empty"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fs, st := Run(c.source, c.tr, c.opt)
			if st != c.wantStatus {
				t.Fatalf("status = %s, want %s (findings %s)", st, c.wantStatus, rules(fs))
			}
			got := rules(fs)
			if c.wantFinding == "" && got != "" {
				t.Fatalf("unexpected findings: %s", got)
			}
			if c.wantFinding != "" && !strings.Contains(got, c.wantFinding) {
				t.Fatalf("findings %q do not include %q", got, c.wantFinding)
			}
		})
	}
}
