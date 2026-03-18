package handle

import (
	"context"
	"exchange_rate/config"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	erp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/exchange_rate"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/leo84927/rabbitmq/v2"
	"github.com/tidwall/gjson"
)

type ExchangeRateHandler interface {
	Handle(ctx context.Context, pair *erp.CurrencyPair)
}

// 法幣
type FiatCurrencyHandler struct {
	publisher rabbitmq.PublishHandler
}

// 虛擬貨幣
type CryptoCurrencyHandler struct {
	publisher rabbitmq.PublishHandler
}

func newHandlerRegistry(publisher rabbitmq.PublishHandler) map[erp.CurrencyType]ExchangeRateHandler {
	return map[erp.CurrencyType]ExchangeRateHandler{
		erp.CurrencyType_CURRENCY_TYPE_FIAT:   &FiatCurrencyHandler{publisher: publisher},
		erp.CurrencyType_CURRENCY_TYPE_CRYPTO: &CryptoCurrencyHandler{publisher: publisher},
	}
}

func (f *FiatCurrencyHandler) Handle(ctx context.Context, pair *erp.CurrencyPair) {
	// build request
	url := fmt.Sprintf("https://v6.exchangerate-api.com/v6/latest/%s", pair.Base)
	log.Println("FiatCurrencyHandler url:", url)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		publishError(
			fmt.Sprintf("FiatCurrencyHandler NewRequest failed, err: %v", err),
			f.publisher,
		)
		return
	}

	// set header
	token := fmt.Sprintf("Bearer %s", config.ExchangeRateApiKey())
	req.Header.Set("Authorization", token)

	// send request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		publishError(
			fmt.Sprintf("FiatCurrencyHandler send request failed, err: %v", err),
			f.publisher,
		)
		return
	}
	defer resp.Body.Close()

	// get response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		publishError(
			fmt.Sprintf("FiatCurrencyHandler get request failed, err: %v", err),
			f.publisher,
		)
		return
	}

	// analyze result
	result := gjson.GetBytes(body, "result").String()
	if result != "success" {
		publishError(
			fmt.Sprintf("FiatCurrencyHandler API result error, msg: %s", gjson.GetBytes(body, "error-type").String()),
			f.publisher,
		)
		return
	}

	for _, counter := range pair.Counter {
		rate := gjson.GetBytes(body, "conversion_rates."+counter)
		if !rate.Exists() {
			publishError(
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
				fmt.Sprintf("FiatCurrencyHandler protojson marshal failed, err: %v", err),
				f.publisher,
			)
			continue
		}

		// Publish 成功的結果到 rabbitmq
		publishSuccess(successBody, f.publisher)
	}
}

func (c *CryptoCurrencyHandler) Handle(ctx context.Context, pair *erp.CurrencyPair) {
	for _, counter := range pair.Counter {
		// build request
		url := fmt.Sprintf("https://api3.binance.com/api/v3/ticker/price?symbol=%s%s", pair.Base, counter)
		log.Println("CryptoCurrencyHandler url:", url)
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			publishError(
				fmt.Sprintf("CryptoCurrencyHandler NewRequest failed, err: %v", err),
				c.publisher,
			)
			return
		}

		// send request
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			publishError(
				fmt.Sprintf("CryptoCurrencyHandler send request failed, err: %v", err),
				c.publisher,
			)
			return
		}
		defer resp.Body.Close()

		// get response
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			publishError(
				fmt.Sprintf("CryptoCurrencyHandler get request failed, err: %v", err),
				c.publisher,
			)
			return
		}

		// analyze result
		symbol := gjson.GetBytes(body, "symbol")
		price := gjson.GetBytes(body, "price")
		if !symbol.Exists() || !price.Exists() {
			publishError(
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
				fmt.Sprintf("CryptoCurrencyHandler protojson marshal failed, err: %v", err),
				c.publisher,
			)
			continue
		}

		// Publish 成功的結果到 rabbitmq
		publishSuccess(successBody, c.publisher)
	}
}

func publishError(errMsg string, publisher rabbitmq.PublishHandler) {
	log.Println(errMsg)

	err := publisher(
		config.GetRabbitMQConfig().Topology.Exchange.Name,
		"telegram.error",
		[]byte(errMsg),
		3,
		5*time.Second,
	)
	if err != nil {
		log.Printf("Publish to telegram failed, err: %v\n", err)
		return
	}

	log.Println("Publish error message to telegram success")
}

func publishSuccess(msg []byte, publisher rabbitmq.PublishHandler) {
	err := publisher(
		config.GetRabbitMQConfig().Topology.Exchange.Name,
		"telegram.success",
		msg,
		3,
		5*time.Second,
	)
	if err != nil {
		log.Printf("Publish to telegram failed, err: %v\n", err)
		return
	}

	log.Println("Publish success message to telegram success")
}
