package comms

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/JrDigitalHub/zeno-work-aoo/pkg/protocol"
)

type VoiceEngine struct {
	whisperURL  string
	styleTTSURL string
}

func NewVoiceEngine(whisperURL, styleTTSURL string) *VoiceEngine {
	return &VoiceEngine{
		whisperURL:  whisperURL,
		styleTTSURL: styleTTSURL,
	}
}

// React listens to the bus. When text context drops, it triggers StyleTTS2 speech generation.
func (v *VoiceEngine) React(ctx context.Context, e protocol.Event) error {
	if e.Source == "SENTINEL_TEXT_OUTPUT" {
		fmt.Printf("🎙️ [VOICE-ENGINE] Intercepted raw text strategy for [%s]. Initiating StyleTTS2 synthesis...\n", e.ID)
		
		outputFile := "outbound_outreach.mp3"
		err := v.Synthesize(ctx, e.Payload, outputFile)
		if err != nil {
			fmt.Printf("❌ [VOICE-ENGINE] Synthesis transaction failed: %v\n", err)
			return err
		}
		fmt.Printf("🔊 [VOICE-ENGINE] Sovereign Audio master baked successfully: %s\n", outputFile)
	}
	return nil
}

// Synthesize sends the text string to your high-fidelity local StyleTTS2 instance
func (v *VoiceEngine) Synthesize(ctx context.Context, text, outputPath string) error {
	// Standard production payload format for local StyleTTS2 inference microservices
	payload := map[string]string{
		"text": text,
	}
	jsonPayload, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", v.styleTTSURL+"/generate", bytes.NewBuffer(jsonPayload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		// Fallback diagnostic if your local docker container isn't running yet
		return fmt.Errorf("local StyleTTS2 server unreachable on %s: %w", v.styleTTSURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("engine returned bad status: %s", resp.Status)
	}

	// Create physical media file on disk
	out, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Stream the raw audio data array straight from the network container onto your disk
	_, err = io.Copy(out, resp.Body)
	return err
}

// Transcribe handles audio ingestion via Whisper endpoints for upcoming audio input integration
func (v *VoiceEngine) Transcribe(audioFilePath string) (string, error) {
	fmt.Printf("🎙️ [VOICE-ENGINE] Ingesting telemetry media: %s via Whisper\n", audioFilePath)
	return "Simulated transcription payload context.", nil
}