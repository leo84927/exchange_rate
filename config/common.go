package config

import "os"

var (
	ServiceName  = os.Getenv("SERVICE_NAME")
	GeminiApiKey = os.Getenv("GEMINI_API_KEY")
)
