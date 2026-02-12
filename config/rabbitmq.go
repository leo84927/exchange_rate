package config

import (
	"os"

	"github.com/leo84927/rabbitmq"
)

var (
	RabbitmqCfg RabbitMQ
)

type RabbitMQ struct {
	Conn  rabbitmq.Config
	Queue rabbitmq.Queue
}

func LoadRabbitMQ() RabbitMQ {
	return RabbitMQ{
		Conn: rabbitmq.Config{
			ServiceName: ServiceName,
			User:        os.Getenv("RABBITMQ_USER"),
			Password:    os.Getenv("RABBITMQ_PASSWORD"),
			Host:        os.Getenv("RABBITMQ_HOST"),
			Port:        os.Getenv("RABBITMQ_PORT"),
			Vhost:       os.Getenv("RABBITMQ_VHOST"),
		},
		Queue: rabbitmq.Queue{
			Name: os.Getenv("RABBITMQ_EXCHANGE_RATE_QUEUE"),
		},
	}
}
