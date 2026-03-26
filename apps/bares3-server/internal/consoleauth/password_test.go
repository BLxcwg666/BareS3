package consoleauth

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestHashAndVerifyPassword(t *testing.T) {
	t.Parallel()

	hash, err := HashPassword("secret-password")
	if err != nil {
		t.Fatalf("HashPassword failed: %v", err)
	}
	matched, err := VerifyPassword("secret-password", hash)
	if err != nil {
		t.Fatalf("VerifyPassword failed: %v", err)
	}
	if !matched {
		t.Fatalf("expected password verification to succeed")
	}

	matched, err = VerifyPassword("wrong-password", hash)
	if err != nil {
		t.Fatalf("VerifyPassword failed: %v", err)
	}
	if matched {
		t.Fatalf("expected password verification to fail")
	}
}

func TestSessionRoundTrip(t *testing.T) {
	t.Parallel()

	hash, err := HashPassword("secret-password")
	if err != nil {
		t.Fatalf("HashPassword failed: %v", err)
	}

	manager, err := NewManager(Options{Username: "admin", PasswordHash: hash, SessionSecret: "test-secret", TTL: time.Minute})
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	session, err := manager.Authenticate("admin", "secret-password")
	if err != nil {
		t.Fatalf("Authenticate failed: %v", err)
	}
	cookie, err := manager.IssueCookie(session)
	if err != nil {
		t.Fatalf("IssueCookie failed: %v", err)
	}

	req := httptest.NewRequest("GET", "/", nil)
	req.AddCookie(cookie)
	loaded, err := manager.SessionFromRequest(req)
	if err != nil {
		t.Fatalf("SessionFromRequest failed: %v", err)
	}
	if loaded.Username != "admin" {
		t.Fatalf("unexpected session username: %s", loaded.Username)
	}
}
