package handle

import (
	"context"
	"exchange_rate/config"
	"fmt"
	"io"
	"log"
	"net/http"

	exchange_rate_proto "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/exchange_rate"

	"github.com/tidwall/gjson"
)

type ExchangeRateHandler interface {
	Handle(ctx context.Context, pair *exchange_rate_proto.CurrencyPair)
}

type FiatCurrencyHandler struct{}

type CryptoCurrencyHandler struct{}

var handlerRegistry = map[string]ExchangeRateHandler{
	"exchange_rate.fiat":   &FiatCurrencyHandler{},
	"exchange_rate.crypto": &CryptoCurrencyHandler{},
}

type ExchangeRate struct {
	BaseCurrency    string `json:"base_currency"`
	CounterCurrency string `json:"counter_currency"`
	Rate            string `json:"rate"`
}

func (f *FiatCurrencyHandler) Handle(ctx context.Context, pair *exchange_rate_proto.CurrencyPair) {
	url := fmt.Sprintf("https://v6.exchangerate-api.com/v6/latest/%s", pair.Base)
	log.Println("GetExchangeRate url:", url)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		// TODO Publish 到 rabbitmq
		log.Printf("GetExchangeRate NewRequest failed, err: %v\n", err)
		return
	}

	token := fmt.Sprintf("Bearer %s", config.ExchangeRateApiKey())
	log.Println("GetExchangeRate token:", token)
	req.Header.Set("Authorization", token)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		// TODO Publish 到 rabbitmq
		log.Printf("GetExchangeRate send request failed, err: %v\n", err)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		// TODO Publish 到 rabbitmq
		log.Printf("GetExchangeRate get request failed, err: %v\n", err)
		return
	}
	log.Printf("Status: %s\n", resp.Status)

	result := gjson.GetBytes(body, "result").String()
	if result != "success" {
		// TODO Publish 到 rabbitmq
		log.Printf("GetExchangeRate result error, msg: %s\n", gjson.GetBytes(body, "error-type").String())
		return
	}

	// return ExchangeRate{
	// 	BaseCurrency:    gjson.GetBytes(body, "base_code").String(),
	// 	CounterCurrency: gjson.GetBytes(body, "conversion_rates").String(),
	// }, nil
}

func (c *CryptoCurrencyHandler) Handle(ctx context.Context, pair *exchange_rate_proto.CurrencyPair) {}
