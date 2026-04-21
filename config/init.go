package config

import (
	cp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/consul"
	coreconfig "github.com/leo84927/core/config"
	"github.com/leo84927/core/rabbitmq"
)

func init() {
	coreconfig.InitFromConsul("EXCHANGE_RATE")

	coreconfig.ServiceName = coreconfig.EnvMap[cp.ExchangeRateEnvKey_EXCHANGE_RATE_SERVICE_NAME.String()]
	ExchangeRateApiKey = coreconfig.EnvMap[cp.ExchangeRateEnvKey_EXCHANGE_RATE_API_KEY.String()]

	coreconfig.LoadBasicRabbitMQ()
	coreconfig.LoadCompleteTopology(rabbitmq.Queue{
		Name: coreconfig.EnvMap[cp.ExchangeRateEnvKey_EXCHANGE_RATE_RABBITMQ_QUEUE.String()],
		Keys: []string{
			coreconfig.EnvMap[cp.ExchangeRateEnvKey_EXCHANGE_RATE_RABBITMQ_KEY.String()],
		},
	})
}
