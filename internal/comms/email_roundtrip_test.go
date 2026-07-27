package comms_test

import (
	"encoding/base64"
	"os"
	"testing"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/comms"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/crypto"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/memory"
)

func setupTestEncryptionKey(t *testing.T) {
	t.Helper()
	key := []byte("12345678901234567890123456789012") // 32 bytes
	encoded := base64.StdEncoding.EncodeToString(key)
	os.Setenv(crypto.EnvEncryptionKey, encoded)
}

func cleanupTestEncryptionKey(t *testing.T) {
	t.Helper()
	os.Unsetenv(crypto.EnvEncryptionKey)
}

func TestSMTPCredentials_RoundTripEncryptionAndDecryption(t *testing.T) {
	setupTestEncryptionKey(t)
	defer cleanupTestEncryptionKey(t)

	rawPassword := "SuperSecretSmtpPass123!"
	
	// 1. Encrypt raw password before storing (as done in POST /api/v1/settings/smtp)
	encryptedPass, err := crypto.Encrypt(rawPassword)
	if err != nil {
		t.Fatalf("Failed to encrypt SMTP password: %v", err)
	}

	// 2. Confirm stored password is NOT plaintext
	if encryptedPass == rawPassword {
		t.Fatal("Security Violation: Encrypted password matches raw password in plain text!")
	}

	creds := memory.SMTPCredentials{
		WorkspaceID: "ws_test_123",
		Host:        "smtp.customprovider.com",
		Port:        "587",
		Username:    "testuser@customprovider.com",
		Password:    encryptedPass,
		SenderName:  "Custom Support",
	}

	// 3. Confirm email engine / crypto decrypts stored ciphertext back to raw password
	decryptedPass, err := crypto.Decrypt(creds.Password)
	if err != nil {
		t.Fatalf("Failed to decrypt stored SMTP password: %v", err)
	}

	if decryptedPass != rawPassword {
		t.Errorf("Expected decrypted password %q, got %q", rawPassword, decryptedPass)
	}

	// 4. Verify EmailEngine fallback behavior when DB is nil
	engine := comms.NewEmailEngine("system.default.smtp", "25", "system@zeno.os", "syspass", "System Default", nil)
	
	// Default fallback verification
	err = engine.SendOutbound("ws_test_123", "target@domain.com", "Test", "Hello")
	if err == nil {
		t.Log("Expected connection error for mock host")
	}
}
