package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	Algorithm          = "AWS4-HMAC-SHA256"
	iso8601BasicFormat = "20060102T150405Z"
	maxPresignExpiry   = 7 * 24 * 60 * 60
	defaultMaxSkew     = 15 * time.Minute
	emptyPayloadSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

type Verifier struct {
	accessKeyID     string
	secretAccessKey string
	region          string
	service         string
	now             func() time.Time
	maxSkew         time.Duration
}

type Identity struct {
	AccessKeyID   string
	Presigned     bool
	SignedHeaders []string
	Timestamp     time.Time
	Scope         CredentialScope
}

type CredentialScope struct {
	Date    string
	Region  string
	Service string
	Term    string
}

type Error struct {
	Status  int
	Code    string
	Message string
	Err     error
}

type authorizationParts struct {
	Credential    string
	SignedHeaders string
	Signature     string
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil {
		return e.Message
	}
	return e.Message + ": " + e.Err.Error()
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func NewVerifier(accessKeyID, secretAccessKey, region, service string) *Verifier {
	return &Verifier{
		accessKeyID:     accessKeyID,
		secretAccessKey: secretAccessKey,
		region:          region,
		service:         service,
		now:             time.Now,
		maxSkew:         defaultMaxSkew,
	}
}

func (v *Verifier) Authenticate(r *http.Request) (*Identity, error) {
	if strings.TrimSpace(r.Header.Get("Authorization")) != "" {
		return v.authenticateHeader(r)
	}
	if hasPresignParams(r.URL.Query()) {
		return v.authenticateQuery(r)
	}
	return nil, &Error{Status: http.StatusForbidden, Code: "AccessDenied", Message: "signature required"}
}

func (v *Verifier) authenticateHeader(r *http.Request) (*Identity, error) {
	parts, err := parseAuthorizationHeader(r.Header.Get("Authorization"))
	if err != nil {
		return nil, err
	}

	scope, accessKeyID, err := parseCredential(parts.Credential)
	if err != nil {
		return nil, err
	}
	if err := v.validateCredentialScope(scope); err != nil {
		return nil, err
	}
	if accessKeyID != v.accessKeyID {
		return nil, &Error{Status: http.StatusForbidden, Code: "InvalidAccessKeyId", Message: "unknown access key id"}
	}

	timestamp, err := parseTimestamp(strings.TrimSpace(r.Header.Get("X-Amz-Date")))
	if err != nil {
		return nil, err
	}
	if err := v.validateTimestamp(timestamp); err != nil {
		return nil, err
	}

	signedHeaders := parseSignedHeaders(parts.SignedHeaders)
	if len(signedHeaders) == 0 {
		return nil, &Error{Status: http.StatusBadRequest, Code: "AuthorizationHeaderMalformed", Message: "signed headers are missing"}
	}

	payloadHash, err := resolvePayloadHash(r, false)
	if err != nil {
		return nil, err
	}

	canonicalRequest, err := canonicalRequest(r, signedHeaders, payloadHash, true)
	if err != nil {
		return nil, err
	}
	stringToSign := buildStringToSign(timestamp, scope, canonicalRequest)
	expectedSignature := hex.EncodeToString(signString(v.secretAccessKey, scope, stringToSign))
	if !secureHexEqual(expectedSignature, parts.Signature) {
		return nil, &Error{Status: http.StatusForbidden, Code: "SignatureDoesNotMatch", Message: "request signature does not match"}
	}

	return &Identity{AccessKeyID: accessKeyID, Presigned: false, SignedHeaders: signedHeaders, Timestamp: timestamp, Scope: scope}, nil
}

func (v *Verifier) authenticateQuery(r *http.Request) (*Identity, error) {
	query := r.URL.Query()
	if strings.TrimSpace(query.Get("X-Amz-Algorithm")) != Algorithm {
		return nil, &Error{Status: http.StatusBadRequest, Code: "AuthorizationQueryParametersError", Message: "unsupported query signing algorithm"}
	}

	scope, accessKeyID, err := parseCredential(strings.TrimSpace(query.Get("X-Amz-Credential")))
	if err != nil {
		return nil, &Error{Status: http.StatusBadRequest, Code: "AuthorizationQueryParametersError", Message: err.Error(), Err: err}
	}
	if err := v.validateCredentialScope(scope); err != nil {
		return nil, err
	}
	if accessKeyID != v.accessKeyID {
		return nil, &Error{Status: http.StatusForbidden, Code: "InvalidAccessKeyId", Message: "unknown access key id"}
	}

	timestamp, err := parseTimestamp(strings.TrimSpace(query.Get("X-Amz-Date")))
	if err != nil {
		return nil, &Error{Status: http.StatusBadRequest, Code: "AuthorizationQueryParametersError", Message: err.Error(), Err: err}
	}

	expiresSeconds, err := strconv.Atoi(strings.TrimSpace(query.Get("X-Amz-Expires")))
	if err != nil || expiresSeconds < 0 || expiresSeconds > maxPresignExpiry {
		return nil, &Error{Status: http.StatusBadRequest, Code: "AuthorizationQueryParametersError", Message: "invalid X-Amz-Expires value"}
	}
	if err := v.validatePresignedTimestamp(timestamp, time.Duration(expiresSeconds)*time.Second); err != nil {
		return nil, err
	}

	signedHeaders := parseSignedHeaders(query.Get("X-Amz-SignedHeaders"))
	if len(signedHeaders) == 0 {
		return nil, &Error{Status: http.StatusBadRequest, Code: "AuthorizationQueryParametersError", Message: "X-Amz-SignedHeaders is required"}
	}

	signature := strings.TrimSpace(query.Get("X-Amz-Signature"))
	if signature == "" {
		return nil, &Error{Status: http.StatusForbidden, Code: "AccessDenied", Message: "X-Amz-Signature is required"}
	}

	payloadHash, err := resolvePayloadHash(r, true)
	if err != nil {
		return nil, err
	}

	canonicalRequest, err := canonicalRequest(r, signedHeaders, payloadHash, false)
	if err != nil {
		return nil, err
	}
	stringToSign := buildStringToSign(timestamp, scope, canonicalRequest)
	expectedSignature := hex.EncodeToString(signString(v.secretAccessKey, scope, stringToSign))
	if !secureHexEqual(expectedSignature, signature) {
		return nil, &Error{Status: http.StatusForbidden, Code: "SignatureDoesNotMatch", Message: "request signature does not match"}
	}

	return &Identity{AccessKeyID: accessKeyID, Presigned: true, SignedHeaders: signedHeaders, Timestamp: timestamp, Scope: scope}, nil
}

func (v *Verifier) validateCredentialScope(scope CredentialScope) error {
	if scope.Region != v.region {
		return &Error{Status: http.StatusBadRequest, Code: "AuthorizationHeaderMalformed", Message: "credential region does not match server region"}
	}
	if scope.Service != v.service {
		return &Error{Status: http.StatusBadRequest, Code: "AuthorizationHeaderMalformed", Message: "credential service must be s3"}
	}
	if scope.Term != "aws4_request" {
		return &Error{Status: http.StatusBadRequest, Code: "AuthorizationHeaderMalformed", Message: "credential scope must end with aws4_request"}
	}
	return nil
}

func (v *Verifier) validateTimestamp(timestamp time.Time) error {
	now := v.now().UTC()
	if timestamp.After(now.Add(v.maxSkew)) || timestamp.Before(now.Add(-v.maxSkew)) {
		return &Error{Status: http.StatusForbidden, Code: "RequestTimeTooSkewed", Message: "request timestamp is outside the allowed skew window"}
	}
	return nil
}

func (v *Verifier) validatePresignedTimestamp(timestamp time.Time, expires time.Duration) error {
	now := v.now().UTC()
	if timestamp.After(now.Add(v.maxSkew)) {
		return &Error{Status: http.StatusForbidden, Code: "RequestTimeTooSkewed", Message: "request timestamp is in the future"}
	}
	if now.After(timestamp.Add(expires).Add(v.maxSkew)) {
		return &Error{Status: http.StatusForbidden, Code: "AccessDenied", Message: "request has expired"}
	}
	return nil
}

func parseAuthorizationHeader(value string) (authorizationParts, error) {
	trimmed := strings.TrimSpace(value)
	if !strings.HasPrefix(trimmed, Algorithm+" ") {
		return authorizationParts{}, &Error{Status: http.StatusBadRequest, Code: "AuthorizationHeaderMalformed", Message: "unsupported authorization algorithm"}
	}

	rest := strings.TrimSpace(strings.TrimPrefix(trimmed, Algorithm))
	parts := strings.Split(rest, ",")
	parsed := authorizationParts{}
	for _, part := range parts {
		key, val, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return authorizationParts{}, &Error{Status: http.StatusBadRequest, Code: "AuthorizationHeaderMalformed", Message: "invalid authorization header"}
		}
		switch key {
		case "Credential":
			parsed.Credential = val
		case "SignedHeaders":
			parsed.SignedHeaders = val
		case "Signature":
			parsed.Signature = val
		}
	}
	if parsed.Credential == "" || parsed.SignedHeaders == "" || parsed.Signature == "" {
		return authorizationParts{}, &Error{Status: http.StatusBadRequest, Code: "AuthorizationHeaderMalformed", Message: "authorization header is missing required fields"}
	}
	return parsed, nil
}

func parseCredential(value string) (CredentialScope, string, error) {
	parts := strings.Split(strings.TrimSpace(value), "/")
	if len(parts) != 5 {
		return CredentialScope{}, "", &Error{Status: http.StatusBadRequest, Code: "AuthorizationHeaderMalformed", Message: "credential scope is invalid"}
	}
	if parts[0] == "" {
		return CredentialScope{}, "", &Error{Status: http.StatusBadRequest, Code: "AuthorizationHeaderMalformed", Message: "access key id is required"}
	}
	return CredentialScope{Date: parts[1], Region: parts[2], Service: parts[3], Term: parts[4]}, parts[0], nil
}

func parseTimestamp(value string) (time.Time, error) {
	if strings.TrimSpace(value) == "" {
		return time.Time{}, &Error{Status: http.StatusBadRequest, Code: "AuthorizationHeaderMalformed", Message: "X-Amz-Date is required"}
	}
	timestamp, err := time.Parse(iso8601BasicFormat, value)
	if err != nil {
		return time.Time{}, &Error{Status: http.StatusBadRequest, Code: "AuthorizationHeaderMalformed", Message: "X-Amz-Date is invalid", Err: err}
	}
	return timestamp.UTC(), nil
}

func parseSignedHeaders(value string) []string {
	raw := strings.Split(strings.TrimSpace(value), ";")
	headers := make([]string, 0, len(raw))
	for _, item := range raw {
		trimmed := strings.ToLower(strings.TrimSpace(item))
		if trimmed == "" {
			continue
		}
		headers = append(headers, trimmed)
	}
	return headers
}

func resolvePayloadHash(r *http.Request, presigned bool) (string, error) {
	if presigned {
		if value := strings.TrimSpace(r.URL.Query().Get("X-Amz-Content-Sha256")); value != "" {
			return value, nil
		}
		return "UNSIGNED-PAYLOAD", nil
	}
	if value := strings.TrimSpace(r.Header.Get("X-Amz-Content-Sha256")); value != "" {
		return value, nil
	}
	if r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodDelete || r.ContentLength == 0 {
		return emptyPayloadSHA256, nil
	}
	return "", &Error{Status: http.StatusBadRequest, Code: "MissingSecurityHeader", Message: "X-Amz-Content-Sha256 header is required"}
}

func canonicalRequest(r *http.Request, signedHeaders []string, payloadHash string, includeSignatureQuery bool) (string, error) {
	canonicalHeaders, err := buildCanonicalHeaders(r, signedHeaders)
	if err != nil {
		return "", err
	}

	query := cloneQuery(r.URL.Query())
	if !includeSignatureQuery {
		query.Del("X-Amz-Signature")
	}

	return strings.Join([]string{
		strings.ToUpper(r.Method),
		canonicalURI(r.URL.Path),
		canonicalQueryString(query),
		canonicalHeaders,
		strings.Join(signedHeaders, ";"),
		payloadHash,
	}, "\n"), nil
}

func buildCanonicalHeaders(r *http.Request, signedHeaders []string) (string, error) {
	var builder strings.Builder
	for _, name := range signedHeaders {
		value, err := signedHeaderValue(r, name)
		if err != nil {
			return "", err
		}
		builder.WriteString(name)
		builder.WriteByte(':')
		builder.WriteString(value)
		builder.WriteByte('\n')
	}
	return builder.String(), nil
}

func signedHeaderValue(r *http.Request, name string) (string, error) {
	if name == "host" {
		host := strings.TrimSpace(r.Host)
		if host == "" {
			host = strings.TrimSpace(r.URL.Host)
		}
		if host == "" {
			return "", &Error{Status: http.StatusBadRequest, Code: "MissingSecurityHeader", Message: "host header is required"}
		}
		return strings.ToLower(host), nil
	}

	values := r.Header.Values(http.CanonicalHeaderKey(name))
	if len(values) == 0 {
		return "", &Error{Status: http.StatusBadRequest, Code: "MissingSecurityHeader", Message: fmt.Sprintf("signed header %s is missing", name)}
	}
	normalized := make([]string, 0, len(values))
	for _, value := range values {
		normalized = append(normalized, normalizeHeaderValue(value))
	}
	return strings.Join(normalized, ","), nil
}

func normalizeHeaderValue(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}

func canonicalURI(value string) string {
	if value == "" {
		return "/"
	}
	var builder strings.Builder
	for i := 0; i < len(value); i++ {
		b := value[i]
		if b == '/' {
			builder.WriteByte('/')
			continue
		}
		if isUnreserved(b) {
			builder.WriteByte(b)
			continue
		}
		builder.WriteString(percentEncodeByte(b))
	}
	return builder.String()
}

func canonicalQueryString(values url.Values) string {
	type pair struct{ key, value string }
	pairs := make([]pair, 0)
	for key, list := range values {
		if len(list) == 0 {
			pairs = append(pairs, pair{key: awsEncode(key), value: ""})
			continue
		}
		for _, value := range list {
			pairs = append(pairs, pair{key: awsEncode(key), value: awsEncode(value)})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].key == pairs[j].key {
			return pairs[i].value < pairs[j].value
		}
		return pairs[i].key < pairs[j].key
	})
	parts := make([]string, 0, len(pairs))
	for _, item := range pairs {
		parts = append(parts, item.key+"="+item.value)
	}
	return strings.Join(parts, "&")
}

func awsEncode(value string) string {
	var builder strings.Builder
	for i := 0; i < len(value); i++ {
		b := value[i]
		if isUnreserved(b) {
			builder.WriteByte(b)
			continue
		}
		builder.WriteString(percentEncodeByte(b))
	}
	return builder.String()
}

func isUnreserved(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || b == '-' || b == '_' || b == '.' || b == '~'
}

func percentEncodeByte(b byte) string {
	const hexChars = "0123456789ABCDEF"
	return "%" + string([]byte{hexChars[b>>4], hexChars[b&0x0F]})
}

func buildStringToSign(timestamp time.Time, scope CredentialScope, canonicalRequest string) string {
	hash := sha256.Sum256([]byte(canonicalRequest))
	return strings.Join([]string{
		Algorithm,
		timestamp.UTC().Format(iso8601BasicFormat),
		strings.Join([]string{scope.Date, scope.Region, scope.Service, scope.Term}, "/"),
		hex.EncodeToString(hash[:]),
	}, "\n")
}

func signString(secretAccessKey string, scope CredentialScope, stringToSign string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretAccessKey), scope.Date)
	kRegion := hmacSHA256(kDate, scope.Region)
	kService := hmacSHA256(kRegion, scope.Service)
	kSigning := hmacSHA256(kService, scope.Term)
	return hmacSHA256(kSigning, stringToSign)
}

func hmacSHA256(key []byte, message string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(message))
	return h.Sum(nil)
}

func secureHexEqual(left, right string) bool {
	left = strings.ToLower(strings.TrimSpace(left))
	right = strings.ToLower(strings.TrimSpace(right))
	if len(left) != len(right) || left == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func cloneQuery(values url.Values) url.Values {
	cloned := make(url.Values, len(values))
	for key, list := range values {
		dup := make([]string, len(list))
		copy(dup, list)
		cloned[key] = dup
	}
	return cloned
}

func hasPresignParams(values url.Values) bool {
	return values.Get("X-Amz-Algorithm") != "" || values.Get("X-Amz-Signature") != "" || values.Get("X-Amz-Credential") != ""
}

var _ error = (*Error)(nil)
