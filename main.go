package main

import (
	"context"
	"exchange_rate/config"
	"exchange_rate/handle"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	env "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/env"
	coreconfig "github.com/leo84927/core/config"
	"github.com/leo84927/core/initialize"
	"github.com/leo84927/core/rabbitmq"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	coreconfig.InitFromRedis(ctx, "EXCHANGE_RATE")
	coreconfig.ServiceName = coreconfig.EnvMap[env.ExchangeRateEnvKey_EXCHANGE_RATE_SERVICE_NAME.String()]
	config.ExchangeRateApiKey = coreconfig.EnvMap[env.ExchangeRateEnvKey_EXCHANGE_RATE_API_KEY.String()]
	config.CoinGeckoApiKey = coreconfig.EnvMap[env.ExchangeRateEnvKey_EXCHANGE_RATE_COINGECKO_API_KEY.String()]
	coreconfig.LoadBasicRabbitMQ()
	coreconfig.LoadCompleteTopology(rabbitmq.Queue{
		Name: coreconfig.EnvMap[env.ExchangeRateEnvKey_EXCHANGE_RATE_RABBITMQ_QUEUE.String()],
		Keys: []string{
			coreconfig.EnvMap[env.ExchangeRateEnvKey_EXCHANGE_RATE_RABBITMQ_KEY.String()],
		},
	})

	app, err := initialize.New(ctx, &initialize.App{
		MQWorker: initialize.MQWorker{
			MsgHandler: handle.MessageHandler,
		},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	defer app.Close(ctx)

	app.Run(ctx)
}
