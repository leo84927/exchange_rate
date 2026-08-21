// Package quoteseam 是 wayfinder ticket #10 的粗胚 —— **丟棄用**，不要 import 進 handle。
//
// 它只回答一個問題：「匯率查詢」與「結果發布」的邊界該切在哪。
// 刻意做到 `go build` 通得過，以確保簽名真的接得上既有的 proto 型別與 core 的 PublishHandler；
// 所有 body 都是 panic stub，不打算能跑。
//
// 前提（已定案，不在本張重議）：
//   - #9：MsgHandler 收斂為 `func(ctx, Message, PublishHandler) error`，requeue 恆 false。
//   - #9：部分完成算整則成功 —— 這條直接決定了下面 Quote 的回傳型別。
package quoteseam

import (
	"context"
	"net/http"
	"time"

	erp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/exchange_rate"
	mqp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/rabbitmq"
	"github.com/leo84927/core/rabbitmq"
)

// ============================================================================
// 候選 A：批次 —— 一則 CurrencyPair 進，一批結果出
// ============================================================================

type BatchQuoter interface {
	Quote(ctx context.Context, pair *erp.CurrencyPair) (QuoteBatch, error)
}

// QuoteBatch 存在的唯一理由：#9 裁定「部分完成算整則成功，失敗的 counter 仍要告警」。
// `([]*erp.ExchangeRate, error)` 表達不了這件事 —— 3 成功 1 失敗時 error 該放什麼？
// 所以邊界上必須有一個型別同時承載兩側。
//
// Quote 自己的 error 則保留給「整批都沒戲」：request 建不起來、HTTP 打不通、認證失敗、
// 供應商回 result != "success"。
type QuoteBatch struct {
	Rates  []*erp.ExchangeRate
	Failed []CounterFailure
}

type CounterFailure struct {
	Counter erp.Currency
	Err     error
}

// ============================================================================
// 候選 B：單筆 —— 一次呼叫一個 base/counter，迴圈移到呼叫端
// ============================================================================
//
// 型別乾淨得多（沒有 QuoteBatch、沒有 CounterFailure），代價是 fiat 供應商從
// 1 次 HTTP 變成 N 次：exchangerate-api 的 /latest/{base} 一次就回傳整張
// conversion_rates 表（見 exchange_rate_handler.go:104-115），單筆介面等於把它丟掉。
type PairQuoter interface {
	Quote(ctx context.Context, base, counter erp.Currency) (*erp.ExchangeRate, error)
}

// ============================================================================
// 供應商 adapter（以候選 A 的簽名寫）
// ============================================================================

// fiatQuoter —— exchangerate-api.com。批次天生合身：一次請求拿回整張表。
type fiatQuoter struct {
	client *http.Client // 注入點：建構子參數（見 newFiatQuoter），不是方法內部 new
	host   string
	apiKey string
}

func newFiatQuoter(client *http.Client, host, apiKey string) *fiatQuoter {
	return &fiatQuoter{client: client, host: host, apiKey: apiKey}
}

func (f *fiatQuoter) Quote(ctx context.Context, pair *erp.CurrencyPair) (QuoteBatch, error) {
	// http.NewRequestWithContext(ctx, ...) —— 現況 exchange_rate_handler.go:63,159 用的是
	// http.NewRequest，ctx 根本沒接上，所以 #17 的 timeout 現在放哪都不會生效。
	panic("prototype")
}

// cryptoQuoter —— api.coingecko.com。
type cryptoQuoter struct {
	client *http.Client
	host   string
	apiKey string
}

func newCryptoQuoter(client *http.Client, host, apiKey string) *cryptoQuoter {
	return &cryptoQuoter{client: client, host: host, apiKey: apiKey}
}

func (c *cryptoQuoter) Quote(ctx context.Context, pair *erp.CurrencyPair) (QuoteBatch, error) {
	panic("prototype")
}

// ----------------------------------------------------------------------------
// 供應商知識：symbol 對照
// ----------------------------------------------------------------------------
//
// 現況（exchange_rate_handler.go:146-157）是**例外式**的 if：只有 USDT 與 BTC 被特判，
// 其他一律 fallback 到 enum 名稱（大寫）。兩個後果：
//   1. 新增第三種 crypto 就是再加一條 if，而漏加不會編譯失敗，只會查詢時查不到。
//   2. `Currency_TWD = 0` 是零值 —— base 沒設就靜靜地當成 TWD 去查。
//
// 變體 A1：查表，缺項即錯誤（把「這個供應商支援哪些幣」變成可窮舉的資料）
var coinGeckoIDs = map[erp.Currency]string{
	erp.Currency_BTC: "bitcoin",
}

var coinGeckoVsCurrencies = map[erp.Currency]string{
	erp.Currency_USD: "usd",
	erp.Currency_TWD: "twd",
	erp.Currency_JPY: "jpy",
	// USDT → "usd"：這**不是** symbol 拼寫差異，是一個領域假設（USDT 錨定 USD 為 1:1）。
	// 現況把它藏在 exchange_rate_handler.go:148 的 if 裡。
	erp.Currency_USDT: "usd",
}

// 變體 A2：維持 switch，但缺項回 error 而非 fallback
func coinGeckoID(c erp.Currency) (string, error) { panic("prototype") }

// ============================================================================
// 結果發布 module
// ============================================================================
//
// 這個介面吸收了 Envelope 組裝 **與** routing key 的選擇（telegram.success / telegram.error），
// 呼叫端因此完全不提 routing key —— 這是本張要裁的其中一格。
type ResultPublisher interface {
	PublishRate(ctx context.Context, rate *erp.ExchangeRate) error
	PublishFailure(ctx context.Context, reason string) error
}

type envelopePublisher struct {
	publish        rabbitmq.PublishHandler // core 的 func(ctx, exchange, key string, body []byte, maxRetries uint, maxElapsedTime time.Duration) error
	exchange       string
	maxRetries     uint
	maxElapsedTime time.Duration
}

func newEnvelopePublisher(publish rabbitmq.PublishHandler, exchange string) *envelopePublisher {
	return &envelopePublisher{
		publish:        publish,
		exchange:       exchange,
		maxRetries:     3,
		maxElapsedTime: 5 * time.Second,
	}
}

func (p *envelopePublisher) PublishRate(ctx context.Context, rate *erp.ExchangeRate) error {
	// EnvelopeType_TELEGRAM_SUCCESS_EXCHANGE_RATE + routing key "telegram.success"
	// 這兩者永遠成對出現 —— 分開讓呼叫端指定，就是給呼叫端一個配錯的機會。
	_ = mqp.EnvelopeType_TELEGRAM_SUCCESS_EXCHANGE_RATE
	panic("prototype")
}

func (p *envelopePublisher) PublishFailure(ctx context.Context, reason string) error {
	_ = mqp.EnvelopeType_TELEGRAM_ERROR
	panic("prototype")
}

// ============================================================================
// 組合：新的 MessageHandler（#9 的 error-only 簽名）
// ============================================================================

// registry 保留成回傳 adapter 的 factory（本張要裁的另一格：留還是拿掉）。
func quoterRegistry(client *http.Client) map[erp.CurrencyType]BatchQuoter {
	return map[erp.CurrencyType]BatchQuoter{
		erp.CurrencyType_CURRENCY_TYPE_FIAT:   newFiatQuoter(client, "", ""),
		erp.CurrencyType_CURRENCY_TYPE_CRYPTO: newCryptoQuoter(client, "", ""),
	}
}

func MessageHandler(ctx context.Context, msg rabbitmq.Message, publish rabbitmq.PublishHandler) error {
	var pair erp.CurrencyPair
	// protojson.Unmarshal(msg.Body, &pair) → return err（#9：訊息無法解析 = 丟棄）

	quoter, ok := quoterRegistry(nil)[pair.Type]
	if !ok {
		// #9：無對應 handler 要回 error，不可 panic
		panic("prototype")
	}

	pub := newEnvelopePublisher(publish, "")

	batch, err := quoter.Quote(ctx, &pair)
	if err != nil {
		// 整批失敗：既要告警（telegram.error），又要回 error 讓 core 標 span 為 Error。
		// **兩者都要做**，這是本張的一格：誰負責 publish 這則告警 —— handler 還是 core？
		_ = pub.PublishFailure(ctx, err.Error())
		return err
	}

	for _, f := range batch.Failed {
		_ = pub.PublishFailure(ctx, f.Err.Error())
	}
	for _, r := range batch.Rates {
		_ = pub.PublishRate(ctx, r)
	}

	// #9：部分完成算整則成功 → 這裡回 nil，即使 batch.Failed 非空。
	return nil
}
