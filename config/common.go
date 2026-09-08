package config

import (
	env "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/env"
)

const (
	defaultFiatURL   = "https://v6.exchangerate-api.com/v6/latest/%s"
	defaultCryptoURL = "https://api.coingecko.com/api/v3/simple/price?vs_currencies=%s&ids=%s"
)

type Config struct {
	FiatAPIKey   string
	CryptoAPIKey string

	FiatURL   string
	CryptoURL string
}

func New(service map[string]string) Config {
	return Config{
		FiatAPIKey:   service[env.ExchangeRateEnvKey_EXCHANGE_RATE_API_KEY.String()],
		CryptoAPIKey: service[env.ExchangeRateEnvKey_EXCHANGE_RATE_COINGECKO_API_KEY.String()],
		FiatURL:      defaultFiatURL,
		CryptoURL:    defaultCryptoURL,
	}
}
