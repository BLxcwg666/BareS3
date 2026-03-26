package consoleauth

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
)

func GenerateSessionSecret() (string, error) {
	buffer := make([]byte, 32)
	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf("generate session secret: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buffer), nil
}
