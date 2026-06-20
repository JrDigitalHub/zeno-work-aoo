# voice-engine/main.py
from fastapi import FastAPI
from pydantic import BaseModel
from gtts import gTTS
import os

app = FastAPI()

class TextPayload(BaseModel):
    text: str
    target_id: str

@app.post("/")
def synthesize_voice(payload: TextPayload):
    try:
        print(f"🎙️ [PYTHON TTS] Received text for target: {payload.target_id}")
        
        # Initialize Google TTS (Free, lightweight)
        tts = gTTS(text=payload.text, lang='en', slow=False)
        
        # Save it to a temporary file
        filename = f"outreach_{payload.target_id.replace('/', '_')}.mp3"
        tts.save(filename)
        
        print(f"✅ [PYTHON TTS] Audio generated: {filename}")
        return {"status": "success", "audio_file": filename}
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

# To run this locally: pip install fastapi uvicorn gtts pydantic
# Then run: uvicorn main:app --host 0.0.0.0 --port 4321