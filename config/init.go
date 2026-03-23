package config

import (
	"os"

	"github.com/joho/godotenv"
)

func init() {
	godotenv.Load()

	ServiceName = os.Getenv("SERVICE_NAME")
	ExchangeRateApiKey = os.Getenv("EXCHANGE_RATE_API_KEY")
	LoadRabbitMQ()
}
