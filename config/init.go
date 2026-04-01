package config

import (
	"log"
	"maps"

	cp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/consul"
	"github.com/leo84927/core/consul"
	"github.com/rotisserie/eris"
)

func init() {
	client, err := consul.NewClient()
	if err != nil {
		log.Fatalf("new consul client failed, err: %v\n", eris.ToJSON(err, true))
	}

	if EnvMap, err = client.List("GLOBAL"); err != nil {
		log.Fatalf("get env from consul failed, err: %v\n", eris.ToJSON(err, true))
	}

	if serviceMap, err := client.List("EXCHANGE_RATE"); err != nil {
		log.Fatalf("get env from consul failed, err: %v\n", eris.ToJSON(err, true))
	} else {
		maps.Copy(EnvMap, serviceMap)
	}

	ServiceName = EnvMap[cp.ExchangeRateEnvKey_EXCHANGE_RATE_SERVICE_NAME.String()]
	ExchangeRateApiKey = EnvMap[cp.ExchangeRateEnvKey_EXCHANGE_RATE_API_KEY.String()]
	AlloyHost = EnvMap[cp.GlobalEnvKey_GLOBAL_ALLOY_HOST.String()]
	AlloyPort = EnvMap[cp.GlobalEnvKey_GLOBAL_ALLOY_PORT.String()]

	LoadRabbitMQ()
}
