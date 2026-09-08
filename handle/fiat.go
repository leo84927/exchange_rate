package handle

import (
	"context"
	"fmt"
	"net/http"

	erp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/exchange_rate"
	"github.com/tidwall/gjson"
)

var fiatSymbols = map[erp.Currency]string{
	erp.Currency_TWD: "TWD",
	erp.Currency_USD: "USD",
	erp.Currency_JPY: "JPY",
}

// 法幣
type fiatQuoter struct {
	client *http.Client
	url    string
	apiKey string
}

func newFiatQuoter(client *http.Client, url, apiKey string) *fiatQuoter {
	return &fiatQuoter{client: client, url: url, apiKey: apiKey}
}

func (f *fiatQuoter) Quote(ctx context.Context, pair *erp.CurrencyPair) (QuoteBatch, error) {
	// 轉換成 ISO 4217 的三位代碼
	base, err := fiatSymbol(pair.Base)
	if err != nil {
		return QuoteBatch{}, fmt.Errorf("fiat base currency: %w", err)
	}

	// 透過 http client 取得匯率
	body, err := fetch(ctx, f.client, fmt.Sprintf(f.url, base), "Authorization", "Bearer "+f.apiKey)
	if err != nil {
		return QuoteBatch{}, fmt.Errorf("exchangerate-api %w", err)
	}

	if result := gjson.GetBytes(body, "result").String(); result != "success" {
		return QuoteBatch{}, fmt.Errorf(
			"exchangerate-api result is %q: %s",
			result,
			gjson.GetBytes(body, "error-type").String(),
		)
	}

	var batch QuoteBatch
	for _, counter := range pair.Counter {
		// 轉換成 ISO 4217 的三位代碼
		symbol, err := fiatSymbol(counter)
		if err != nil {
			batch.Failed = append(batch.Failed, CounterFailure{
				Counter: counter,
				Err:     fmt.Errorf("fiat counter currency: %w", err),
			})
			continue
		}

		// 解析匯率
		rate := gjson.GetBytes(body, "conversion_rates."+symbol)
		if !rate.Exists() {
			batch.Failed = append(batch.Failed, CounterFailure{
				Counter: counter,
				Err:     fmt.Errorf("exchangerate-api has no rate for %s/%s", base, symbol),
			})
			continue
		}

		batch.Rates = append(batch.Rates, &erp.ExchangeRate{
			BaseCurrency:    pair.Base,
			CounterCurrency: counter,
			// 法幣最多五位小數
			Rate: fmt.Sprintf("%.5f", rate.Float()),
		})
	}

	return batch, nil
}

func fiatSymbol(c erp.Currency) (string, error) {
	s, ok := fiatSymbols[c]
	if !ok {
		return "", fmt.Errorf("%s: %w", c, ErrUnsupportedCurrency)
	}

	return s, nil
}
