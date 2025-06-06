package utils

import (
	"crypto/rand"
	"encoding/hex"
)

// generateCode returns a 128 bit (16 byte) random code, hex-encoded (32 chars).
func GenerateCode() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
