package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
)

const EnvEncryptionKey = "SMTP_CREDENTIALS_ENCRYPTION_KEY"

// getKey retrieves the base64-encoded 32-byte key from the environment variable.
func getKey() ([]byte, error) {
	keyStr := os.Getenv(EnvEncryptionKey)
	if keyStr == "" {
		return nil, fmt.Errorf("environment variable %s is not set", EnvEncryptionKey)
	}

	key, err := base64.StdEncoding.DecodeString(keyStr)
	if err != nil {
		return nil, fmt.Errorf("failed to decode encryption key from base64: %w", err)
	}

	if len(key) != 32 {
		return nil, fmt.Errorf("encryption key must be exactly 32 bytes (256 bits), got %d bytes", len(key))
	}

	return key, nil
}

// Encrypt encrypts a plaintext string using AES-256-GCM.
// It generates a random 12-byte nonce, prepends it to the ciphertext,
// and returns the base64-encoded result.
func Encrypt(plaintext string) (string, error) {
	key, err := getKey()
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher block: %w", err)
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %w", err)
	}

	nonce := make([]byte, aesGCM.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("failed to generate random nonce: %w", err)
	}

	ciphertext := aesGCM.Seal(nil, nonce, []byte(plaintext), nil)

	// Combine nonce + ciphertext
	combined := append(nonce, ciphertext...)

	return base64.StdEncoding.EncodeToString(combined), nil
}

// Decrypt decrypts a base64-encoded ciphertext string using AES-256-GCM.
func Decrypt(ciphertextStr string) (string, error) {
	key, err := getKey()
	if err != nil {
		return "", err
	}

	combined, err := base64.StdEncoding.DecodeString(ciphertextStr)
	if err != nil {
		return "", fmt.Errorf("failed to decode ciphertext from base64: %w", err)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher block: %w", err)
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %w", err)
	}

	nonceSize := aesGCM.NonceSize()
	if len(combined) < nonceSize {
		return "", errors.New("ciphertext too short to contain a valid nonce")
	}

	nonce := combined[:nonceSize]
	ciphertext := combined[nonceSize:]

	plaintext, err := aesGCM.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("decryption failed (possible key mismatch or data tampering): %w", err)
	}

	return string(plaintext), nil
}
