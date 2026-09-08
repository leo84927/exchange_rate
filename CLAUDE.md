# exchange_rate
接收 center 排程的任務，查詢匯率後將結果透過 RabbitMQ 轉發給 telegram 服務

## 架構

```
config/common.go              ← Config 具名結構＋New，從 core 的 Settings.Service 取值；無全域變數

handle/message_handler.go     ← Worker：consumer 進入點，解析 CurrencyPair 後依 CurrencyType 分派 Quoter
handle/quote.go               ← Quoter 介面、QuoteBatch、ErrUnsupportedCurrency、registry
handle/fiat.go                ← 法幣 adapter（exchangerate-api.com）＋支援幣別表
handle/crypto.go              ← 虛擬貨幣 adapter（api.coingecko.com）＋ids／vs_currencies 兩張表
handle/http.go                ← fetch：單次對外請求，帶 ctx、不重試
handle/publisher.go           ← ResultPublisher：Envelope 組裝與 routing key 的配對

main.go                       ← config.Load → 建 http.Client → 組 Worker → initialize.New
```

## 訊息處理流程

```
RabbitMQ message（CurrencyPair proto）
  → Worker.Handle 解析 CurrencyPair（解析失敗 → 回 error，訊息丟棄）
  → registry 依 CurrencyType 取 Quoter（查不到 → 回 error，不 panic）
      FIAT   → fiatQuoter   → exchangerate-api.com
      CRYPTO → cryptoQuoter → CoinGecko
  → Quoter.Quote 以「一則 CurrencyPair」為單位回傳 QuoteBatch
  → 包成 Envelope 發布回 RabbitMQ：
      Rates  → routing key: telegram.success（EnvelopeType: TELEGRAM_SUCCESS_EXCHANGE_RATE）
      Failed → routing key: telegram.error（EnvelopeType: TELEGRAM_ERROR）
```

下游由 telegram 服務接手，見根目錄 `CONTEXT.md` 的訊息流向。

## 設定鍵

| 鍵 | 用途 |
|---|---|
| `EXCHANGE_RATE_SERVICE_NAME` | 服務名稱 |
| `EXCHANGE_RATE_API_KEY` | exchangerate-api.com 的 API key |
| `EXCHANGE_RATE_RABBITMQ_QUEUE` | 訂閱的 queue 名稱 |
| `EXCHANGE_RATE_RABBITMQ_KEY` | routing key |
| `EXCHANGE_RATE_COINGECKO_API_KEY` | api.coingecko.com 的 API key |

## 測試

```sh
go test ./... -race -v -count=1
golangci-lint run ./...
```
