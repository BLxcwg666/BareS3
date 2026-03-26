package consoleauth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/argon2"
)

const (
	defaultMemory      = 64 * 1024
	defaultIterations  = 3
	defaultParallelism = 2
	defaultSaltLength  = 16
	defaultKeyLength   = 32
)

func HashPassword(password string) (string, error) {
	if password == "" {
		return "", fmt.Errorf("password must not be empty")
	}
	salt := make([]byte, defaultSaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("generate password salt: %w", err)
	}
	hash := argon2.IDKey([]byte(password), salt, defaultIterations, defaultMemory, defaultParallelism, defaultKeyLength)
	return fmt.Sprintf("$argon2id$v=19$m=%d,t=%d,p=%d$%s$%s", defaultMemory, defaultIterations, defaultParallelism, base64.RawStdEncoding.EncodeToString(salt), base64.RawStdEncoding.EncodeToString(hash)), nil
}

func VerifyPassword(password, encoded string) (bool, error) {
	params, salt, expected, err := parseHash(encoded)
	if err != nil {
		return false, err
	}
	actual := argon2.IDKey([]byte(password), salt, params.iterations, params.memory, params.parallelism, uint32(len(expected)))
	return subtle.ConstantTimeCompare(actual, expected) == 1, nil
}

type hashParams struct {
	memory      uint32
	iterations  uint32
	parallelism uint8
}

func parseHash(encoded string) (hashParams, []byte, []byte, error) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[1] != "argon2id" {
		return hashParams{}, nil, nil, fmt.Errorf("password hash is not a valid argon2id PHC string")
	}
	if parts[2] != "v=19" {
		return hashParams{}, nil, nil, fmt.Errorf("unsupported argon2 version")
	}

	params := hashParams{}
	for _, item := range strings.Split(parts[3], ",") {
		key, value, ok := strings.Cut(item, "=")
		if !ok {
			return hashParams{}, nil, nil, fmt.Errorf("invalid argon2 parameter %q", item)
		}
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return hashParams{}, nil, nil, fmt.Errorf("invalid argon2 parameter %q", item)
		}
		switch key {
		case "m":
			params.memory = uint32(parsed)
		case "t":
			params.iterations = uint32(parsed)
		case "p":
			params.parallelism = uint8(parsed)
		}
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return hashParams{}, nil, nil, fmt.Errorf("decode argon2 salt: %w", err)
	}
	hash, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return hashParams{}, nil, nil, fmt.Errorf("decode argon2 hash: %w", err)
	}
	return params, salt, hash, nil
}
