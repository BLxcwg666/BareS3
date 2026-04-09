package sigv4

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type PresignInput struct {
	Method  string
	URL     *url.URL
	Expires time.Duration
	Now     time.Time
}

type PresignResult struct {
	URL       string    `json:"url"`
	ExpiresAt time.Time `json:"expires_at"`
	Method    string    `json:"method"`
}

func (v *Verifier) Presign(input PresignInput) (PresignResult, error) {
	if input.URL == nil {
		return PresignResult{}, fmt.Errorf("presign url is required")
	}

	method := strings.ToUpper(strings.TrimSpace(input.Method))
	if method == "" {
		method = http.MethodGet
	}
	if method != http.MethodGet && method != http.MethodPut && method != http.MethodHead && method != http.MethodDelete {
		return PresignResult{}, fmt.Errorf("unsupported presign method %q", method)
	}

	expires := input.Expires
	if expires <= 0 {
		expires = 15 * time.Minute
	}
	if expires > time.Duration(maxPresignExpiry)*time.Second {
		return PresignResult{}, fmt.Errorf("presign expiry exceeds %d seconds", maxPresignExpiry)
	}

	now := input.Now.UTC()
	if now.IsZero() {
		now = v.now().UTC()
	}
	if strings.TrimSpace(v.accessKeyID) == "" || strings.TrimSpace(v.secretAccessKey) == "" {
		return PresignResult{}, fmt.Errorf("presign credentials are not configured")
	}

	cloned := *input.URL
	query := cloneQuery(cloned.Query())
	query.Set("X-Amz-Algorithm", Algorithm)
	query.Set("X-Amz-Credential", v.accessKeyID+"/"+now.Format("20060102")+"/"+v.region+"/"+v.service+"/aws4_request")
	query.Set("X-Amz-Date", now.Format(iso8601BasicFormat))
	query.Set("X-Amz-Expires", strconv.Itoa(int(expires.Seconds())))
	query.Set("X-Amz-SignedHeaders", "host")
	cloned.RawQuery = query.Encode()

	req := &http.Request{
		Method: method,
		URL:    &cloned,
		Host:   cloned.Host,
		Header: make(http.Header),
	}

	scope := CredentialScope{Date: now.Format("20060102"), Region: v.region, Service: v.service, Term: "aws4_request"}
	canonical, err := canonicalRequest(req, []string{"host"}, "UNSIGNED-PAYLOAD", false)
	if err != nil {
		return PresignResult{}, err
	}
	stringToSign := buildStringToSign(now, scope, canonical)
	query.Set("X-Amz-Signature", hex.EncodeToString(signString(v.secretAccessKey, scope, stringToSign)))
	cloned.RawQuery = query.Encode()

	return PresignResult{URL: cloned.String(), ExpiresAt: now.Add(expires), Method: method}, nil
}
