package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	env "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/env"
	coreconfig "github.com/leo84927/core/v2/config"
	"github.com/leo84927/core/v2/initialize"

	"exchange_rate/config"
	"exchange_rate/handle"
)

/*
 * 對外查匯率的單次上限
 *
 * worker 同步化之後，一個掛住的對外請求會占住一格 prefetch；四格用完服務就靜默停止消費。
 */
const supplierTimeout = 5 * time.Second

// 一個 client 給兩個 adapter：超時只有一個落點，改一次兩個供應商一起改
func newSupplierClient() *http.Client {
	return &http.Client{Timeout: supplierTimeout}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// os.Exit 不跑 defer，所以整個啟動流程收在 run 裡，讓 Close 有機會配對執行
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	settings, err := coreconfig.Load(ctx, coreconfig.Spec{
		Prefix:         "EXCHANGE_RATE",
		ServiceNameKey: env.ExchangeRateEnvKey_EXCHANGE_RATE_SERVICE_NAME,
		Queue: &coreconfig.QueueKeys{
			NameKey:    env.ExchangeRateEnvKey_EXCHANGE_RATE_RABBITMQ_QUEUE,
			RoutingKey: env.ExchangeRateEnvKey_EXCHANGE_RATE_RABBITMQ_KEY,
		},
		ServiceKeys: []fmt.Stringer{
			env.ExchangeRateEnvKey_EXCHANGE_RATE_API_KEY,
			env.ExchangeRateEnvKey_EXCHANGE_RATE_COINGECKO_API_KEY,
		},
	})
	if err != nil {
		return err
	}

	// &handle.Worker
	worker := handle.NewWorker(
		newSupplierClient(),
		config.New(settings.Service),
		settings.RabbitMQ.Topology.Exchange.Name,
	)

	app, err := initialize.New(ctx, settings, &initialize.App{
		MQWorker: initialize.MQWorker{
			MsgHandler: worker.Handle,
		},
	})
	if err != nil {
		return err
	}
	defer app.Close(ctx)

	return app.Run(ctx)
}
