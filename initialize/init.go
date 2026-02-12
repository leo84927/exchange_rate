package initialize

import (
	"context"
	"exchange_rate/config"
	"exchange_rate/handle"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/leo84927/rabbitmq"
)

var (
	consumer *rabbitmq.Consumer
)

func Init() {
	// 初始化 rabbitmq 連線
	config.RabbitmqCfg = config.LoadRabbitMQ()
	rabbitmq.SetConn(config.RabbitmqCfg.Conn)

	// 初始化 consumer
	consumer = rabbitmq.NewConsumer(config.RabbitmqCfg.Queue.Name, "")

	fmt.Println("Init Success")
}

func Start() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	consumer.WaitForConsume(ctx, handle.MessageHandler)
}
