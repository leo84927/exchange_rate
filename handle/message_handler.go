package handle

import (
	"context"
	"encoding/json"
	"log"

	"github.com/leo84927/rabbitmq/v2"
	"golang.org/x/sync/errgroup"
)

type CurrencyPair struct {
	Base    string   `json:"base"`
	Counter []string `json:"counter"`
}

func MessageHandler(ctx context.Context, msg rabbitmq.Message) (requeue bool, err error) {
	log.Printf("=== Start processing message ===")
	log.Printf("Message body: %s", msg.Body)
	defer log.Printf("=== End processing message ===")

	var currencyPair CurrencyPair
	if err = json.Unmarshal(msg.Body, &currencyPair); err != nil {
		log.Printf("MessageHandler json unmarshal failed, err: %v\n", err)
		return false, err
	}

	group, groupCtx := errgroup.WithContext(ctx)

	// 一個 Base 可能會有多個 Counter，併發取得匯率
	for _, counter := range currencyPair.Counter {
		group.Go(func() error {
			exchangeRate, err := GetExchangeRate(groupCtx, currencyPair.Base, counter)
			if err != nil {
				return err
			}

			log.Printf("%s/%s exchange rate: %s", currencyPair.Base, counter, exchangeRate)
			// TODO PUblish 到 telegram，要開 goroutine
			return nil
		})
	}

	// 等待所有 pair 都取得匯率
	if err := group.Wait(); err != nil {
		return true, err
	}

	return false, nil
}
