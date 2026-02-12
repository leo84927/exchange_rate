package main

import (
	"exchange_rate/initialize"

	"github.com/joho/godotenv"
	"github.com/leo84927/rabbitmq"
)

func init() {
	godotenv.Load()
}

func main() {
	initialize.Init()
	defer func() {
		rabbitmq.Close()
	}()

	initialize.Start()
}
