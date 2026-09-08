package handle

import (
	"context"
	"errors"
	"net/http"

	erp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/exchange_rate"

	"exchange_rate/config"
)

var ErrUnsupportedCurrency = errors.New("currency not supported by supplier")

/*
 * QuoteBatch 存在理由是「部分完成算整則成功，失敗的 counter 仍要告警」。
 *
 * 回傳型別沿用 Contract repo 的 erp.ExchangeRate，不另建內部領域型別。
 */
type QuoteBatch struct {
	Rates  []*erp.ExchangeRate
	Failed []CounterFailure
}

type CounterFailure struct {
	Counter erp.Currency
	Err     error
}

type Quoter interface {
	// func 回傳的 error 代表查詢失敗。若查詢成功但匯率轉換有問題，放在 QuoteBatch.Failed.CounterFailure。
	Quote(ctx context.Context, pair *erp.CurrencyPair) (QuoteBatch, error)
}

// http client 由呼叫端建立並傳入，兩個 adapter 共用同一個，超時限制因此相同
func newQuoterRegistry(client *http.Client, cfg config.Config) map[erp.CurrencyType]Quoter {
	return map[erp.CurrencyType]Quoter{
		// &handle.fiatQuoter
		erp.CurrencyType_CURRENCY_TYPE_FIAT:   newFiatQuoter(client, cfg.FiatURL, cfg.FiatAPIKey),
		// &handle.cryptoQuoter
		erp.CurrencyType_CURRENCY_TYPE_CRYPTO: newCryptoQuoter(client, cfg.CryptoURL, cfg.CryptoAPIKey),
	}
}
