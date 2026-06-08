# exchange_rate
接收 center 排程的任務，查詢匯率後將結果透過 RabbitMQ 轉發給 telegram 服務

## 架構

```
config/init.go                ← init() 從 Consul 載入設定、建立 RabbitMQ topology
config/common.go              ← ExchangeRateApiKey

handle/message_handler.go     ← RabbitMQ consumer 進入點，解析 CurrencyPair 後依 CurrencyType 分派 handler
handle/exchange_rate_handler.go ← Fiat / Crypto handler 實作、publishError / publishSuccess

main.go                       ← 使用 core/initialize.App 啟動，註冊 consumer worker
```

## 訊息處理流程

```
RabbitMQ message（CurrencyPair proto）
  → MessageHandler 解析 CurrencyPair
  → 依 CurrencyType 分派：
      FIAT   → FiatCurrencyHandler   → exchangerate-api.com（需 API key）
      CRYPTO → CryptoCurrencyHandler → Binance public API
  → 對每個 counter currency 查詢匯率
  → 包成 Envelope 發布回 RabbitMQ：
      成功 → routing key: telegram.success（EnvelopeType: TELEGRAM_SUCCESS_EXCHANGE_RATE）
      失敗 → routing key: telegram.error（EnvelopeType: TELEGRAM_ERROR）
  → telegram 服務消費後發送至 Telegram chat
```

## 外部 API

| 類型 | API | 認證 |
|---|---|---|
| 法幣 | `https://v6.exchangerate-api.com/v6/latest/{base}` | Bearer token（Consul `EXCHANGE_RATE_API_KEY`） |
| 加密貨幣 | `https://api3.binance.com/api/v3/ticker/price?symbol={base}{counter}` | 無 |

回應皆以 `gjson` 解析 JSON。

## Consul 設定鍵

| 鍵 | 用途 |
|---|---|
| `EXCHANGE_RATE_SERVICE_NAME` | 服務名稱 |
| `EXCHANGE_RATE_API_KEY` | exchangerate-api.com 的 API key |
| `EXCHANGE_RATE_RABBITMQ_QUEUE` | 訂閱的 queue 名稱 |
| `EXCHANGE_RATE_RABBITMQ_KEY` | routing key |

## 依賴

- `github.com/tidwall/gjson` — JSON 路徑查詢（解析 API 回應）
- `github.com/leo84927/core` — 共用基礎建設
- `buf.build/gen/go/.../scheduler` — proto 定義（CurrencyPair、ExchangeRate、Envelope）
