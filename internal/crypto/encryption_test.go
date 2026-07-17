package crypto

import (
	"encoding/base64"
	"os"
	"testing"
)

// Helper function to set up a valid 32-byte key in environment
func setupValidKey(t *testing.T) {
	t.Helper()
	// 32-byte key: "abcdefghijklmnopqrstuvwxyz123456"
	key := []byte("abcdefghijklmnopqrstuvwxyz123456")
	encoded := base64.StdEncoding.EncodeToString(key)
	os.Setenv(EnvEncryptionKey, encoded)
}

func cleanupKey(t *testing.T) {
	t.Helper()
	os.Unsetenv(EnvEncryptionKey)
}

func TestEncryptDecrypt_Success(t *testing.T) {
	setupValidKey(t)
	defer cleanupKey(t)

	plaintext := "sensitive SMTP password"
	ciphertext, err := Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	if ciphertext == "" {
		t.Fatal("expected ciphertext to be non-empty")
	}

	decrypted, err := Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	if decrypted != plaintext {
		t.Errorf("expected decrypted text %q, got %q", plaintext, decrypted)
	}
}

func TestEncryptDecrypt_MissingKey(t *testing.T) {
	cleanupKey(t)

	_, err := Encrypt("plaintext")
	if err == nil {
		t.Error("expected error when key is not set in environment, got nil")
	}

	_, err = Decrypt("ciphertext")
	if err == nil {
		t.Error("expected error when key is not set in environment, got nil")
	}
}

func TestEncryptDecrypt_InvalidKeyLength(t *testing.T) {
	// Too short key (16 bytes instead of 32)
	shortKey := []byte("too_short_key_16")
	encoded := base64.StdEncoding.EncodeToString(shortKey)
	os.Setenv(EnvEncryptionKey, encoded)
	defer cleanupKey(t)

	_, err := Encrypt("plaintext")
	if err == nil {
		t.Error("expected error when key is not 32 bytes, got nil")
	}
}

func TestDecrypt_TamperedCiphertext(t *testing.T) {
	setupValidKey(t)
	defer cleanupKey(t)

	plaintext := "original sensitive content"
	ciphertext, err := Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	// Decode the base64 ciphertext
	decoded, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		t.Fatalf("failed to decode ciphertext: %v", err)
	}

	// Tamper with the last byte of the ciphertext
	decoded[len(decoded)-1] ^= 0xFF
	tamperedCiphertext := base64.StdEncoding.EncodeToString(decoded)

	_, err = Decrypt(tamperedCiphertext)
	if err == nil {
		t.Error("expected decryption to fail for tampered ciphertext, but it succeeded")
	}
}
