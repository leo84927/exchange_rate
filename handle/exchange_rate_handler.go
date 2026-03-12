package handle

import (
	"context"
	"encoding/json"
	"exchange_rate/config"
	"fmt"
	"log"

	"google.golang.org/genai"
)

type ExchangeRate struct {
	BaseCurrency    string `json:"base_currency"`
	CounterCurrency string `json:"counter_currency"`
	Rate            string `json:"rate"`
}

func GetExchangeRate(ctx context.Context, base, counter string) (ExchangeRate, error) {
	client, err := genai.NewClient(ctx, &genai.ClientConfig{
		APIKey: config.GeminiApiKey,
	})
	if err != nil {
		log.Printf("GetExchangeRate NewClient failed, err: %v\n", err)
		return ExchangeRate{}, err
	}

	prompt := fmt.Sprintf("Please get the %s/%s exchange rate from https://www.google.com/finance/quote/%s-%s", base, counter, base, counter)
	config := &genai.GenerateContentConfig{
		// 指定 MIME type
		ResponseMIMEType: "application/json",
		// 指定資料格式
		ResponseJsonSchema: map[string]any{
			"type": "object",
			// 必須有的 key
			"required": []string{
				"base_currency",
				"counter_currency",
				"rate",
			},
			// 指定各個 key 的型別
			"properties": map[string]any{
				"base_currency": map[string]any{
					"type":        "string",
					"description": "base currency",
				},
				"counter_currency": map[string]any{
					"type":        "string",
					"description": "counter currency",
				},
				"rate": map[string]any{
					"type":        "string",
					"description": "rate",
				},
			},
		},
	}
	result, err := client.Models.GenerateContent(
		ctx,
		"gemini-3.1-flash-lite-preview",
		genai.Text(prompt),
		config,
	)
	if err != nil {
		log.Printf("GetExchangeRate GenerateContent failed, err: %v\n", err)
		return ExchangeRate{}, err
	}

	log.Println("GenerateContent Success, result:", result.Text())

	// publish 前解析一次，確保格式正確
	var exchangeRate ExchangeRate
	err = json.Unmarshal([]byte(result.Text()), &exchangeRate)
	if err != nil {
		log.Printf("GetExchangeRate json unmarshal failed, err: %v\n", err)
		return ExchangeRate{}, err
	}

	return exchangeRate, nil
}
