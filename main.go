package main

import (
	"context"
	"exchange_rate/config"
	"exchange_rate/handle"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/leo84927/rabbitmq/v2"
	"golang.org/x/sync/errgroup"
)

func init() {
	godotenv.Load()
	config.LoadRabbitMQ()
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cm := rabbitmq.NewConnectionManager(config.GetRabbitMQConfig().Config)
	defer cm.Close()

	consumer := cm.NewConsumer(config.GetRabbitMQConfig().Queue.Name, "", 5, 20*time.Second)

	group, groupCtx := errgroup.WithContext(ctx)

	group.Go(func() error {
		return cm.WatchConnAndRetry(groupCtx)
	})

	group.Go(func() error {
		return consumer.WaitForConsume(groupCtx, handle.MessageHandler)
	})

	// 等待所有 goroutine 結束
	if err := group.Wait(); err != nil {
		log.Println("exit with error:", err)
		return
	}

	log.Println("normal shutdown")
}
