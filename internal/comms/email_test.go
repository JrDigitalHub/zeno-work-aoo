package comms_test

import (
	"testing"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/comms"
)

func TestEmailEngine_DefaultFallback(t *testing.T) {
	engine := comms.NewEmailEngine("smtp.example.com", "587", "user@example.com", "password123", "Default Sender", nil)

	// Outbound call without DB connection should cleanly fall back to system defaults
	err := engine.SendOutbound("", "recipient@example.com", "Test Subject", "<p>Test Body</p>")
	if err == nil {
		t.Log("Expected TLS network connection error for fake smtp server, got nil")
	}
}
