package httpx

import (
	"strings"
	"testing"
)

func TestRedactQueryStringMasksSensitiveParameters(t *testing.T) {
	t.Parallel()

	redacted := redactQueryString("plain=ok&token=abc123&X-Amz-Signature=deadbeef&x-amz-credential=keyscope")
	if !strings.Contains(redacted, "plain=ok") {
		t.Fatalf("expected plain query parameter to remain visible, got %q", redacted)
	}
	for _, secret := range []string{"abc123", "deadbeef", "keyscope"} {
		if strings.Contains(redacted, secret) {
			t.Fatalf("expected sensitive value %q to be redacted in %q", secret, redacted)
		}
	}
	for _, key := range []string{"token=%5Bredacted%5D", "X-Amz-Signature=%5Bredacted%5D", "x-amz-credential=%5Bredacted%5D"} {
		if !strings.Contains(redacted, key) {
			t.Fatalf("expected redacted query to contain %q, got %q", key, redacted)
		}
	}
}

func TestRedactQueryStringHandlesInvalidInput(t *testing.T) {
	t.Parallel()

	if got := redactQueryString("%%%"); got != "[invalid]" {
		t.Fatalf("unexpected invalid query redaction: %q", got)
	}
}
