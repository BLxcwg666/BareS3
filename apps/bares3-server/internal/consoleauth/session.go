package consoleauth

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const CookieName = "bares3_admin_session"

type Manager struct {
	username       string
	passwordHash   string
	sessionSecret  []byte
	ttl            time.Duration
	now            func() time.Time
	passwordMarker string
}

type Options struct {
	Username      string
	PasswordHash  string
	SessionSecret string
	TTL           time.Duration
}

type Session struct {
	Username  string    `json:"username"`
	ExpiresAt time.Time `json:"expires_at"`
}

type sessionPayload struct {
	Username       string `json:"u"`
	ExpiresUnix    int64  `json:"e"`
	PasswordMarker string `json:"p"`
}

func NewManager(options Options) (*Manager, error) {
	if strings.TrimSpace(options.Username) == "" {
		return nil, fmt.Errorf("console username must not be empty")
	}
	if strings.TrimSpace(options.PasswordHash) == "" {
		return nil, fmt.Errorf("console password hash must not be empty")
	}
	if strings.TrimSpace(options.SessionSecret) == "" {
		return nil, fmt.Errorf("console session secret must not be empty")
	}
	if options.TTL <= 0 {
		options.TTL = 7 * 24 * time.Hour
	}

	marker := sha256.Sum256([]byte(options.PasswordHash))
	return &Manager{
		username:       options.Username,
		passwordHash:   options.PasswordHash,
		sessionSecret:  []byte(options.SessionSecret),
		ttl:            options.TTL,
		now:            time.Now,
		passwordMarker: hex.EncodeToString(marker[:8]),
	}, nil
}

func (m *Manager) Username() string {
	return m.username
}

func (m *Manager) Authenticate(username, password string) (Session, error) {
	if username != m.username {
		return Session{}, fmt.Errorf("invalid credentials")
	}
	matched, err := VerifyPassword(password, m.passwordHash)
	if err != nil {
		return Session{}, err
	}
	if !matched {
		return Session{}, fmt.Errorf("invalid credentials")
	}
	expiresAt := m.now().UTC().Add(m.ttl)
	return Session{Username: m.username, ExpiresAt: expiresAt}, nil
}

func (m *Manager) IssueCookie(session Session) (*http.Cookie, error) {
	payload := sessionPayload{Username: session.Username, ExpiresUnix: session.ExpiresAt.UTC().Unix(), PasswordMarker: m.passwordMarker}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	encodedPayload := base64.RawURLEncoding.EncodeToString(body)
	signature := hex.EncodeToString(signHMAC(m.sessionSecret, encodedPayload))
	return &http.Cookie{
		Name:     CookieName,
		Value:    encodedPayload + "." + signature,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Expires:  session.ExpiresAt.UTC(),
		MaxAge:   int(time.Until(session.ExpiresAt).Seconds()),
		Secure:   false,
	}, nil
}

func (m *Manager) ClearCookie() *http.Cookie {
	return &http.Cookie{
		Name:     CookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Expires:  time.Unix(0, 0).UTC(),
		MaxAge:   -1,
		Secure:   false,
	}
}

func (m *Manager) SessionFromRequest(r *http.Request) (Session, error) {
	cookie, err := r.Cookie(CookieName)
	if err != nil {
		return Session{}, err
	}
	payload, signature, ok := strings.Cut(cookie.Value, ".")
	if !ok || payload == "" || signature == "" {
		return Session{}, fmt.Errorf("invalid session cookie")
	}
	expected := hex.EncodeToString(signHMAC(m.sessionSecret, payload))
	if subtle.ConstantTimeCompare([]byte(strings.ToLower(signature)), []byte(strings.ToLower(expected))) != 1 {
		return Session{}, fmt.Errorf("invalid session signature")
	}
	body, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return Session{}, err
	}
	decoded := sessionPayload{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return Session{}, err
	}
	if decoded.Username != m.username || decoded.PasswordMarker != m.passwordMarker {
		return Session{}, fmt.Errorf("session no longer matches configured credentials")
	}
	expiresAt := time.Unix(decoded.ExpiresUnix, 0).UTC()
	if m.now().UTC().After(expiresAt) {
		return Session{}, fmt.Errorf("session expired")
	}
	return Session{Username: decoded.Username, ExpiresAt: expiresAt}, nil
}

func signHMAC(secret []byte, payload string) []byte {
	h := hmac.New(sha256.New, secret)
	_, _ = h.Write([]byte(payload))
	return h.Sum(nil)
}
