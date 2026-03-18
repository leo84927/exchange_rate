package handle

import (
	"context"
	"log"

	erp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/exchange_rate"

	"github.com/leo84927/rabbitmq/v2"
	"google.golang.org/protobuf/encoding/protojson"
)

func MessageHandler(ctx context.Context, msg rabbitmq.Message, publisher rabbitmq.PublishHandler) (requeue bool, err error) {
	log.Printf("=== Start processing message ===")
	log.Printf("Message body: %s", msg.Body)
	defer log.Printf("=== End processing message ===")

	var currencyPair erp.CurrencyPair
	if err = protojson.Unmarshal(msg.Body, &currencyPair); err != nil {
		log.Printf("MessageHandler json unmarshal failed, err: %v\n", err)
		return false, err
	}

	// 透過 factory 取得特定的 handler。這裡要開 goroutine，避免阻塞 consumer
	exchangeRateHandler := newHandlerRegistry(publisher)[currencyPair.Type]
	go exchangeRateHandler.Handle(ctx, &currencyPair)

	return false, nil
}
