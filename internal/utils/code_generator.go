package utils

import (
	"crypto/rand"
	"encoding/hex"
)

// GenerateCode produces a unique, cryptographically secure random code.
// This code is 128 bits (16 bytes) long and is then hex-encoded,
// resulting in a 32-character string. This is what I used for
// checkout codes to ensure they are unpredictable and unique.
func GenerateCode() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
