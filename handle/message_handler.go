package handle

import (
	"context"
	"log/slog"

	erp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/exchange_rate"

	"github.com/leo84927/core/rabbitmq"
	"github.com/rotisserie/eris"
	"google.golang.org/protobuf/encoding/protojson"
)

func MessageHandler(ctx context.Context, msg rabbitmq.Message, publisher rabbitmq.PublishHandler) (requeue bool, err error) {
	slog.Info("=== processing message start ===")
	slog.Info(
		"received message from RabbitMQ",
		"message", msg.Body,
	)
	defer slog.Info("=== processing message finished ===")

	var currencyPair erp.CurrencyPair
	if err = protojson.Unmarshal(msg.Body, &currencyPair); err != nil {
		slog.Error(
			"message handler json unmarshal failed",
			"error", eris.ToJSON(err, true),
		)
		return false, err
	}

	// 透過 factory 取得特定的 handler。這裡要開 goroutine，避免阻塞 consumer
	exchangeRateHandler := newHandlerRegistry(publisher)[currencyPair.Type]
	go exchangeRateHandler.Handle(ctx, &currencyPair)

	return false, nil
}
