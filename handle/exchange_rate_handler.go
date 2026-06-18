package handle

import (
	"context"
	"exchange_rate/config"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	erp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/exchange_rate"
	mqp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/rabbitmq"
	"google.golang.org/protobuf/encoding/protojson"

	coreconfig "github.com/leo84927/core/config"
	"github.com/leo84927/core/rabbitmq"
	"github.com/rotisserie/eris"
	"github.com/tidwall/gjson"
)

type supplier struct {
	host string
}

type ExchangeRateHandler interface {
	Handle(ctx context.Context, pair *erp.CurrencyPair)
}

// 法幣
type FiatCurrencyHandler struct {
	publisher rabbitmq.PublishHandler
	supplier  supplier
}

// 虛擬貨幣
type CryptoCurrencyHandler struct {
	publisher rabbitmq.PublishHandler
	supplier  supplier
}

func newHandlerRegistry(publisher rabbitmq.PublishHandler) map[erp.CurrencyType]ExchangeRateHandler {
	return map[erp.CurrencyType]ExchangeRateHandler{
		erp.CurrencyType_CURRENCY_TYPE_FIAT: &FiatCurrencyHandler{
			publisher: publisher,
			supplier: supplier{
				host: "https://v6.exchangerate-api.com/v6/latest/%s",
			},
		},
		erp.CurrencyType_CURRENCY_TYPE_CRYPTO: &CryptoCurrencyHandler{
			publisher: publisher,
			supplier: supplier{
				host: "https://api.coingecko.com/api/v3/simple/price?vs_currencies=%s&ids=%s",
			},
		},
	}
}

func (f *FiatCurrencyHandler) Handle(ctx context.Context, pair *erp.CurrencyPair) {
	// build request
	url := fmt.Sprintf(f.supplier.host, pair.Base)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		publishError(
			ctx,
			fmt.Sprintf("FiatCurrencyHandler NewRequest failed, err: %v", err),
			f.publisher,
		)
		return
	}

	// set header
	token := fmt.Sprintf("Bearer %s", config.ExchangeRateApiKey)
	req.Header.Set("Authorization", token)

	// send request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		publishError(
			ctx,
			fmt.Sprintf("FiatCurrencyHandler send request failed, err: %v", err),
			f.publisher,
		)
		return
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	// get response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		publishError(
			ctx,
			fmt.Sprintf("FiatCurrencyHandler get request failed, err: %v", err),
			f.publisher,
		)
		return
	}

	// analyze result
	result := gjson.GetBytes(body, "result").String()
	if result != "success" {
		publishError(
			ctx,
			fmt.Sprintf("FiatCurrencyHandler API result error, msg: %s", gjson.GetBytes(body, "error-type").String()),
			f.publisher,
		)
		return
	}

	for _, counter := range pair.Counter {
		rate := gjson.GetBytes(body, "conversion_rates."+counter.String())
		if !rate.Exists() {
			publishError(
				ctx,
				"FiatCurrencyHandler exchange rate not exists",
				f.publisher,
			)
			continue
		}

		successBody, err := protojson.Marshal(&erp.ExchangeRate{
			BaseCurrency:    pair.Base,
			CounterCurrency: counter,
			Rate:            fmt.Sprintf("%.5f", rate.Float()),
		})
		if err != nil {
			publishError(
				ctx,
				fmt.Sprintf("FiatCurrencyHandler protojson marshal failed, err: %v", err),
				f.publisher,
			)
			continue
		}

		// Publish 成功的結果到 rabbitmq
		publishSuccess(ctx, successBody, f.publisher)
	}
}

func (c *CryptoCurrencyHandler) Handle(ctx context.Context, pair *erp.CurrencyPair) {
	for _, counter := range pair.Counter {
		var supplierCounter, supplierBase string
		// build request
		if counter == erp.Currency_USDT {
			supplierCounter = "usd"
		} else {
			supplierCounter = counter.String()
		}
		if pair.Base == erp.Currency_BTC {
			supplierBase = "bitcoin"
		} else {
			supplierBase = pair.Base.String()
		}
		url := fmt.Sprintf(c.supplier.host, supplierCounter, supplierBase)
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			publishError(
				ctx,
				fmt.Sprintf("CryptoCurrencyHandler NewRequest failed, err: %v", err),
				c.publisher,
			)
			return
		}

		// set header
		req.Header.Set("x-cg-demo-api-key", config.CoinGeckoApiKey)

		// send request
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			publishError(
				ctx,
				fmt.Sprintf("CryptoCurrencyHandler send request failed, err: %v", err),
				c.publisher,
			)
			return
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		// get response
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			publishError(
				ctx,
				fmt.Sprintf("CryptoCurrencyHandler get request failed, err: %v", err),
				c.publisher,
			)
			return
		}

		// analyze result
		price := gjson.GetBytes(body, supplierBase+"."+supplierCounter)
		if !price.Exists() {
			publishError(
				ctx,
				fmt.Sprintf("CryptoCurrencyHandler API result error, body: %s", string(body)),
				c.publisher,
			)
			return
		}

		successBody, err := protojson.Marshal(&erp.ExchangeRate{
			BaseCurrency:    pair.Base,
			CounterCurrency: counter,
			Rate:            price.String(),
		})
		if err != nil {
			publishError(
				ctx,
				fmt.Sprintf("CryptoCurrencyHandler protojson marshal failed, err: %v", err),
				c.publisher,
			)
			continue
		}

		// Publish 成功的結果到 rabbitmq
		publishSuccess(ctx, successBody, c.publisher)
	}
}

func publishError(ctx context.Context, errMsg string, publisher rabbitmq.PublishHandler) {
	body, err := protojson.Marshal(&mqp.Envelope{
		Type:   mqp.EnvelopeType_TELEGRAM_ERROR,
		Data:   errMsg,
		SentAt: time.Now().Unix(),
	})
	if err != nil {
		slog.Error(
			"publish to telegram, protojson Marshal failed",
			"error", eris.ToJSON(err, true),
		)
		return
	}

	err = publisher(
		ctx,
		coreconfig.GetRabbitMQConfig().Topology.Exchange.Name,
		"telegram.error",
		body,
		3,
		5*time.Second,
	)
	if err != nil {
		slog.Error(
			"publish to telegram failed",
			"error", eris.ToJSON(err, true),
		)
		return
	}

	slog.Info("publish error message to telegram finish")
}

func publishSuccess(ctx context.Context, msg []byte, publisher rabbitmq.PublishHandler) {
	body, err := protojson.Marshal(&mqp.Envelope{
		Type:   mqp.EnvelopeType_TELEGRAM_SUCCESS_EXCHANGE_RATE,
		Data:   string(msg),
		SentAt: time.Now().Unix(),
	})
	if err != nil {
		slog.Error(
			"publish to telegram, protojson Marshal failed",
			"error", eris.ToJSON(err, true),
		)
		return
	}

	err = publisher(
		ctx,
		coreconfig.GetRabbitMQConfig().Topology.Exchange.Name,
		"telegram.success",
		body,
		3,
		5*time.Second,
	)
	if err != nil {
		slog.Error(
			"publish to telegram failed",
			"error", eris.ToJSON(err, true),
		)
		return
	}

	slog.Info("publish success message to telegram finish")
}
