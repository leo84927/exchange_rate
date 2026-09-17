package handle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	erp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/exchange_rate"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/leo84927/core/v2/logger"
	"github.com/leo84927/core/v2/rabbitmq"

	"exchange_rate/config"
)

// Worker 是 consumer 的進入點。Handle 同步執行，回傳代表工作真的做完了，core 才會 Ack
type Worker struct {
	quoters  map[erp.CurrencyType]Quoter
	exchange string
}

func NewWorker(client *http.Client, cfg config.Config, exchange string) *Worker {
	return &Worker{
		quoters:  newQuoterRegistry(client, cfg),
		exchange: exchange,
	}
}

/*
 * "CurrencyPair": {
 *   "base": 1,
 *   "counter": [2, 3],
 *   "type": 1
 * }
 */
func (w *Worker) Handle(ctx context.Context, msg rabbitmq.Message, publish rabbitmq.PublishHandler) error {
	slog.Info("=== processing message start ===")
	defer slog.Info("=== processing message finished ===")

	var pair erp.CurrencyPair
	if err := protojson.Unmarshal(msg.Body, &pair); err != nil {
		// 連 CurrencyPair 都拼不出來，就沒有可以告警的對象，只能丟棄
		return fmt.Errorf("unmarshal currency pair: %w", err)
	}

	// counter 不可為空
	if len(pair.Counter) == 0 {
		return fmt.Errorf("currency pair with base %s has no counter currency", pair.Base)
	}

	// type 不存在時失敗
	quoter, ok := w.quoters[pair.Type]
	if !ok {
		return fmt.Errorf("no quoter for currency type %s", pair.Type)
	}

	// &handle.envelopePublisher
	var publisher ResultPublisher = newEnvelopePublisher(publish, w.exchange)

	// batch 與 quoteErr 同時有意義，所以先把已經查到的送出去，再回報整批的失敗
	batch, quoteErr := quoter.Quote(ctx, &pair)
	publishErr := publishBatch(ctx, publisher, batch)

	if quoteErr != nil {
		if publishErr != nil {
			logger.Error(ctx, "publish partial results to telegram failed", publishErr)
		}

		/*
		 * 查詢（整批）失敗時（quoteErr != nil），告警與回傳 error 兩件都做
		 * 前者讓使用者知道，後者讓 core 把 span 標為 Error
		 */
		if err := publisher.PublishFailure(ctx, quoteErr.Error()); err != nil {
			logger.Error(ctx, "publish batch failure to telegram failed", err)
		}

		return quoteErr
	}

	/*
	 * 部分完成算整則成功，Failed 非空仍回 nil。
	 *
	 * 但「發布失敗」不能一併吞掉，回 nil 會讓 core 走 Ack，所以這裡回傳 publishErr，若不為 nil 就讓 core 走 Nack。
	 */
	return publishErr
}

// 把 QuoteBatch 的兩側都送出去，並聚合發布過程中的失敗
func publishBatch(ctx context.Context, publisher ResultPublisher, batch QuoteBatch) error {
	var errs []error

	// 失敗
	for _, failure := range batch.Failed {
		if err := publisher.PublishFailure(ctx, failure.Err.Error()); err != nil {
			errs = append(errs, fmt.Errorf("publish %s failure: %w", failure.Counter, err))
		}
	}

	// 成功
	for _, rate := range batch.Rates {
		if err := publisher.PublishRate(ctx, rate); err != nil {
			errs = append(errs, fmt.Errorf("publish %s rate: %w", rate.CounterCurrency, err))
		}
	}

	return errors.Join(errs...)
}
