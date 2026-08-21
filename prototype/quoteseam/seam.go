// Package quoteseam 是 wayfinder ticket #10 的粗胚 —— **丟棄用**，不要 import 進 handle。
// 它承載本張定案後的介面形狀，供後續實作 session 對照。
//
// `go build` / `go vet` 通得過，所有 body 都是 panic stub。
//
// 已定案的前提（不在本張重議）：
//   - #8：併發歸 core，prefetch 4，work-then-Ack。
//   - #9：MsgHandler 收斂為 `func(ctx, Message, PublishHandler) error`，requeue 恆 false。
//   - #9：部分完成算整則成功 —— 直接決定了 QuoteBatch 的存在。
package quoteseam

import (
	"context"
	"errors"
	"net/http"
	"time"

	erp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/exchange_rate"
	mqp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/rabbitmq"
	"github.com/leo84927/core/rabbitmq"
)

// ============================================================================
// 匯率查詢
// ============================================================================

// Quoter 的工作單位是**一則 CurrencyPair**，不是一個 base/counter 組合。
//
// 選批次而非單筆的理由：exchangerate-api 的 /latest/{base} 一次請求回傳整張
// conversion_rates 表（現況 exchange_rate_handler.go:104-115 就是這樣用的），
// 單筆介面會把這個能力丟掉，fiat 從 1 次 HTTP 變 N 次。
type Quoter interface {
	Quote(ctx context.Context, pair *erp.CurrencyPair) (QuoteBatch, error)
}

// QuoteBatch 存在的唯一理由是 #9 的「部分完成算整則成功，失敗的 counter 仍要告警」。
// `([]*erp.ExchangeRate, error)` 表達不了 3 成功 1 失敗 —— error 放了就違反「整則成功」，
// 不放就丟失告警。所以邊界上必須有一個型別同時承載兩側。
type QuoteBatch struct {
	Rates  []*erp.ExchangeRate
	Failed []CounterFailure
}

type CounterFailure struct {
	Counter erp.Currency
	Err     error
}

// Quote 自己的 error 只表達「整批都沒戲」：request 建不起來、HTTP 不通、認證失敗、
// 供應商回 result != "success"、base 幣別不受供應商支援。
var ErrUnsupportedCurrency = errors.New("currency not supported by supplier")

// ============================================================================
// 供應商 adapter
// ============================================================================
//
// 回傳型別沿用 Contract repo 的 erp.ExchangeRate，不另建內部領域型別。
// Rate 是 string，**精度由各 adapter 自行決定** —— 這是刻意的差異，不是待統一的不一致：
// 虛擬貨幣需要比法幣更多的位數。

// ----------------------------------------------------------------------------
// fiat —— exchangerate-api.com
// ----------------------------------------------------------------------------

type fiatQuoter struct {
	client *http.Client
	host   string
	apiKey string
}

// client 是必填建構子參數，不是 functional option：adapter 沒有 client 就不能work，
// 必填參數誠實表達這件事。順帶讓 #17 的 timeout 有唯一的落點（main.go 建一個
// 帶 Timeout 的 client 傳進來），而 option 的預設值 http.DefaultClient 的 Timeout 是 0。
func newFiatQuoter(client *http.Client, host, apiKey string) *fiatQuoter {
	return &fiatQuoter{client: client, host: host, apiKey: apiKey}
}

// fiatSymbols：exchangerate-api 用 ISO 4217 代碼，與 enum 名稱**恰好一致**，
// 所以這張表的價值不在轉換而在**窮舉支援範圍** —— 沒有它，center 誤把 BTC 標成
// FIAT 時會去打 /latest/BTC，現在則直接 ErrUnsupportedCurrency。
var fiatSymbols = map[erp.Currency]string{
	erp.Currency_TWD: "TWD",
	erp.Currency_USD: "USD",
	erp.Currency_JPY: "JPY",
}

func fiatSymbol(c erp.Currency) (string, error) {
	s, ok := fiatSymbols[c]
	if !ok {
		return "", ErrUnsupportedCurrency
	}
	return s, nil
}

func (f *fiatQuoter) Quote(ctx context.Context, pair *erp.CurrencyPair) (QuoteBatch, error) {
	// http.NewRequestWithContext(ctx, ...) —— 現況 :63,:159 用的是 http.NewRequest，
	// ctx 從來沒接上 HTTP 呼叫，所以 #17 的 timeout 不管放哪都不會生效。
	//
	// base 不支援 → return QuoteBatch{}, ErrUnsupportedCurrency（整批失敗）
	// 某個 counter 不支援 → 進 Failed，其餘照走（#9：部分完成）
	// Rate: fmt.Sprintf("%.5f", rate.Float()) —— 法幣五位小數
	panic("prototype")
}

// ----------------------------------------------------------------------------
// crypto —— api.coingecko.com
// ----------------------------------------------------------------------------

type cryptoQuoter struct {
	client *http.Client
	host   string
	apiKey string
}

func newCryptoQuoter(client *http.Client, host, apiKey string) *cryptoQuoter {
	return &cryptoQuoter{client: client, host: host, apiKey: apiKey}
}

// CoinGecko 的兩個參數語意不同，所以是兩張表，不是一張。
// ids = 被報價的資產；vs_currencies = 報價幣別。
var coinGeckoIDs = map[erp.Currency]string{
	erp.Currency_BTC: "bitcoin",
}

var coinGeckoVsCurrencies = map[erp.Currency]string{
	erp.Currency_USD: "usd",
	erp.Currency_TWD: "twd",
	erp.Currency_JPY: "jpy",
	// CoinGecko 沒有 USDT/BTC 這個交易對，vs_currencies 不吃 usdt，
	// 因此以 USD 報價代替。這是**供應商限制**造成的替代，不是拼寫差異 ——
	// 後果是回報的 BTC/USDT 實際上是 BTC/USD 的價格。
	erp.Currency_USDT: "usd",
}

func coinGeckoID(c erp.Currency) (string, error) {
	s, ok := coinGeckoIDs[c]
	if !ok {
		return "", ErrUnsupportedCurrency
	}
	return s, nil
}

func coinGeckoVsCurrency(c erp.Currency) (string, error) {
	s, ok := coinGeckoVsCurrencies[c]
	if !ok {
		return "", ErrUnsupportedCurrency
	}
	return s, nil
}

func (c *cryptoQuoter) Quote(ctx context.Context, pair *erp.CurrencyPair) (QuoteBatch, error) {
	// 現況 :183 的 defer resp.Body.Close() 在 for 迴圈裡 —— defer 累積到函式返回才執行，
	// N 個 counter 就是 N 個 body 同時開著。每個 counter 的請求要抽成獨立函式。
	//
	// Rate: price.String() —— 原樣帶走，虛擬貨幣不截斷
	panic("prototype")
}

// ----------------------------------------------------------------------------
// registry
// ----------------------------------------------------------------------------

// 保留 newHandlerRegistry 這個 factory，改成回傳 adapter。
// map 查找的 ok 正好給了 #9 要求的「無對應 CurrencyType 回 error、不可 panic」。
func quoterRegistry(client *http.Client) map[erp.CurrencyType]Quoter {
	return map[erp.CurrencyType]Quoter{
		erp.CurrencyType_CURRENCY_TYPE_FIAT:   newFiatQuoter(client, "", ""),
		erp.CurrencyType_CURRENCY_TYPE_CRYPTO: newCryptoQuoter(client, "", ""),
	}
}

// ============================================================================
// 結果發布
// ============================================================================

// ResultPublisher 吸收 Envelope 組裝 **與** routing key 的選擇。
// EnvelopeType 與 routing key 永遠成對出現（TELEGRAM_SUCCESS_EXCHANGE_RATE ↔
// telegram.success），分開讓呼叫端指定就是給它一個配錯的機會。
type ResultPublisher interface {
	PublishRate(ctx context.Context, rate *erp.ExchangeRate) error
	PublishFailure(ctx context.Context, reason string) error
}

type envelopePublisher struct {
	publish        rabbitmq.PublishHandler
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
	_ = mqp.EnvelopeType_TELEGRAM_SUCCESS_EXCHANGE_RATE // ↔ "telegram.success"
	panic("prototype")
}

func (p *envelopePublisher) PublishFailure(ctx context.Context, reason string) error {
	_ = mqp.EnvelopeType_TELEGRAM_ERROR // ↔ "telegram.error"
	panic("prototype")
}

// ============================================================================
// 組合
// ============================================================================

func MessageHandler(ctx context.Context, msg rabbitmq.Message, publish rabbitmq.PublishHandler) error {
	var pair erp.CurrencyPair
	// protojson.Unmarshal(msg.Body, &pair) → return err（#9：訊息無法解析 = 丟棄）

	pub := newEnvelopePublisher(publish, "")

	quoter, ok := quoterRegistry(nil)[pair.Type]
	if !ok {
		// #9：無對應 handler 回 error，不可 panic
		return errors.New("no quoter for currency type")
	}

	batch, err := quoter.Quote(ctx, &pair)
	if err != nil {
		// 整批失敗：告警與 return err **兩件都做**。
		// PublishFailure 讓使用者知道，return err 讓 core 標 span 為 Error
		// （#9：SetStatus(codes.Error) 是 Grafana 上唯一標記失敗的地方）。
		_ = pub.PublishFailure(ctx, err.Error())
		return err
	}

	for _, f := range batch.Failed {
		_ = pub.PublishFailure(ctx, f.Err.Error())
	}
	for _, r := range batch.Rates {
		_ = pub.PublishRate(ctx, r)
	}

	// #9：部分完成算整則成功 → 即使 Failed 非空也回 nil
	return nil
}
