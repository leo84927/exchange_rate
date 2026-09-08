package handle

import (
	"context"
	"fmt"
	"net/http"

	erp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/exchange_rate"
	"github.com/tidwall/gjson"
)

/*
 * CoinGecko 的兩個參數語意不同，所以是兩張表不是一張：
 * ids 是被報價的資產，vs_currencies 是報價幣別。
 */
var coinGeckoIDs = map[erp.Currency]string{
	erp.Currency_BTC: "bitcoin",
}

/*
 * USDT 對到 usd 是供應商限制造成的替代，不是拼寫差異：CoinGecko 沒有 BTC/USDT 這個交易對，
 * vs_currencies 不吃 usdt。後果是回報的 BTC/USDT 實際上是 BTC/USD 的價格。
 */
var coinGeckoVsCurrencies = map[erp.Currency]string{
	erp.Currency_USD:  "usd",
	erp.Currency_TWD:  "twd",
	erp.Currency_JPY:  "jpy",
	erp.Currency_USDT: "usd",
}

// 虛擬貨幣
type cryptoQuoter struct {
	client *http.Client
	url    string
	apiKey string
}

func newCryptoQuoter(client *http.Client, url, apiKey string) *cryptoQuoter {
	return &cryptoQuoter{client: client, url: url, apiKey: apiKey}
}

func (c *cryptoQuoter) Quote(ctx context.Context, pair *erp.CurrencyPair) (QuoteBatch, error) {
	// 轉換成 CoinGecko 的 id
	id, err := coinGeckoID(pair.Base)
	if err != nil {
		return QuoteBatch{}, fmt.Errorf("crypto base currency: %w", err)
	}

	var batch QuoteBatch
	for _, counter := range pair.Counter {
		// 轉換成 CoinGecko 的 vs_currency
		vs, err := coinGeckoVsCurrency(counter)
		if err != nil {
			batch.Failed = append(batch.Failed, CounterFailure{
				Counter: counter,
				Err:     fmt.Errorf("crypto counter currency: %w", err),
			})
			continue
		}

		// 透過 http client 取得匯率
		body, err := fetch(ctx, c.client, fmt.Sprintf(c.url, vs, id), "x-cg-demo-api-key", c.apiKey)
		if err != nil {
			// 前幾個 counter 已經查到的匯率不該被這一次失敗吃掉
			return batch, fmt.Errorf("coingecko %w", err)
		}

		price := gjson.GetBytes(body, id+"."+vs)
		if !price.Exists() {
			batch.Failed = append(batch.Failed, CounterFailure{
				Counter: counter,
				Err:     fmt.Errorf("coingecko has no price for %s/%s", id, vs),
			})
			continue
		}

		batch.Rates = append(batch.Rates, &erp.ExchangeRate{
			BaseCurrency:    pair.Base,
			CounterCurrency: counter,
			// 虛擬貨幣不截斷小數點，原樣返回
			Rate: price.String(),
		})
	}

	return batch, nil
}

func coinGeckoID(c erp.Currency) (string, error) {
	s, ok := coinGeckoIDs[c]
	if !ok {
		return "", fmt.Errorf("%s: %w", c, ErrUnsupportedCurrency)
	}

	return s, nil
}

func coinGeckoVsCurrency(c erp.Currency) (string, error) {
	s, ok := coinGeckoVsCurrencies[c]
	if !ok {
		return "", fmt.Errorf("%s: %w", c, ErrUnsupportedCurrency)
	}

	return s, nil
}
