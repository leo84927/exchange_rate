package handle

import (
	"context"
	"fmt"
	"time"

	erp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/exchange_rate"
	mqp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/rabbitmq"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/leo84927/core/rabbitmq"
)

const (
	successRoutingKey = "telegram.success"
	errorRoutingKey   = "telegram.error"
)

type envelopePublisher struct {
	publish  rabbitmq.PublishHandler
	exchange string
}

// 吸收 Envelope 組裝與 routing key 的選擇，呼叫端只需要認得「成功」與「失敗」
type ResultPublisher interface {
	PublishRate(ctx context.Context, rate *erp.ExchangeRate) error
	PublishFailure(ctx context.Context, reason string) error
}

func newEnvelopePublisher(publish rabbitmq.PublishHandler, exchange string) *envelopePublisher {
	return &envelopePublisher{publish: publish, exchange: exchange}
}

func (p *envelopePublisher) PublishRate(ctx context.Context, rate *erp.ExchangeRate) error {
	body, err := protojson.Marshal(rate)
	if err != nil {
		return fmt.Errorf("marshal exchange rate: %w", err)
	}

	return p.send(ctx, successRoutingKey, mqp.EnvelopeType_TELEGRAM_SUCCESS_EXCHANGE_RATE, string(body))
}

func (p *envelopePublisher) PublishFailure(ctx context.Context, reason string) error {
	return p.send(ctx, errorRoutingKey, mqp.EnvelopeType_TELEGRAM_ERROR, reason)
}

func (p *envelopePublisher) send(ctx context.Context, key string, envelopeType mqp.EnvelopeType, data string) error {
	body, err := protojson.Marshal(&mqp.Envelope{
		Type:   envelopeType,
		Data:   data,
		SentAt: time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	// 重試預算收在 core 的連線管理器裡（{3, 5s}）
	return p.publish(ctx, p.exchange, key, body)
}
