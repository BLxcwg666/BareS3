package storage

import (
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"unicode"
)

const (
	controlDirName     = ".bares3"
	metaDirName        = "meta"
	bucketMetaName     = "bucket.json"
	bucketHistoryName  = "usage-history.json"
	escapedPrefix      = "~x"
	emptySegmentToken  = "~e"
	dotSegmentToken    = "~d"
	dotDotSegmentToken = "~dd"
)

func validateBucketName(name string) error {
	if len(name) < 3 || len(name) > 63 {
		return fmt.Errorf("%w: bucket name must be 3-63 chars", ErrInvalidBucketName)
	}
	if strings.Contains(name, "..") {
		return fmt.Errorf("%w: bucket name must not contain consecutive dots", ErrInvalidBucketName)
	}
	if strings.HasPrefix(name, "-") || strings.HasSuffix(name, "-") || strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") {
		return fmt.Errorf("%w: bucket name must not start or end with dot or hyphen", ErrInvalidBucketName)
	}
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '.' {
			continue
		}
		return fmt.Errorf("%w: bucket name contains unsupported character %q", ErrInvalidBucketName, r)
	}
	return nil
}

func encodeObjectKey(key string) ([]string, error) {
	if key == "" {
		return nil, fmt.Errorf("%w: object key must not be empty", ErrInvalidObjectKey)
	}

	rawSegments := strings.Split(key, "/")
	encoded := make([]string, 0, len(rawSegments))
	for _, segment := range rawSegments {
		value, err := encodeSegment(segment)
		if err != nil {
			return nil, err
		}
		encoded = append(encoded, value)
	}
	return encoded, nil
}

func encodeSegment(segment string) (string, error) {
	if len(segment) > 240 {
		return "", fmt.Errorf("%w: key segment too long", ErrInvalidObjectKey)
	}

	switch segment {
	case "":
		return emptySegmentToken, nil
	case ".":
		return dotSegmentToken, nil
	case "..":
		return dotDotSegmentToken, nil
	}

	if isSafeSegment(segment) {
		return segment, nil
	}

	return escapedPrefix + hex.EncodeToString([]byte(segment)), nil
}

func isSafeSegment(segment string) bool {
	if segment == controlDirName || strings.HasPrefix(segment, "~") {
		return false
	}
	if strings.HasSuffix(segment, ".") || strings.HasSuffix(segment, " ") {
		return false
	}
	if isWindowsReservedName(segment) {
		return false
	}

	for _, r := range segment {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r):
			continue
		case strings.ContainsRune("-_. @+=,()[]{}", r):
			continue
		case r < 0x20:
			return false
		case strings.ContainsRune(`<>:"/\\|?*`, r):
			return false
		default:
			continue
		}
	}

	return true
}

func isWindowsReservedName(segment string) bool {
	base := strings.TrimSpace(segment)
	if index := strings.IndexRune(base, '.'); index >= 0 {
		base = base[:index]
	}

	switch strings.ToUpper(base) {
	case "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9":
		return true
	default:
		return false
	}
}

func joinPath(parts ...string) string {
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			continue
		}
		cleaned = append(cleaned, part)
	}
	return filepath.Join(cleaned...)
}
