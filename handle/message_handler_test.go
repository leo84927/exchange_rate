package handle

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	erp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/exchange_rate"
	mqp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/rabbitmq"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/leo84927/core/v2/rabbitmq"

	"exchange_rate/config"
)

/*
 * seam 落在 worker 進入點：對外 HTTP 用 httptest 頂住，真的 adapter 進測試。
 *
 * Quoter 與 ResultPublisher 兩個介面依然存在，但不是測試的主要入口 —— 替身只在
 * 「透過 HTTP 觀察不到」時才用得上，而下面每一條斷言都觀察得到。
 */

const testExchange = "job.exchange"

// ─────────────────────────────────────────────
// 測試替身
// ─────────────────────────────────────────────

type recordedRequest struct {
	url    *url.URL
	header http.Header
}

/*
 * supplierStub 記下供應商端收到的每一次請求。
 *
 * 需要 mutex：httptest 的 handler 跑在自己的 goroutine 上，而超時那條測試會讓 handler
 * 一路活到測試結束，與斷言真的併發。
 */
type supplierStub struct {
	mu       sync.Mutex
	requests []recordedRequest
	url      string
}

func newSupplierStub(t *testing.T, respond http.HandlerFunc) *supplierStub {
	t.Helper()

	stub := &supplierStub{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stub.mu.Lock()
		stub.requests = append(stub.requests, recordedRequest{url: r.URL, header: r.Header.Clone()})
		stub.mu.Unlock()

		respond(w, r)
	}))
	t.Cleanup(server.Close)

	stub.url = server.URL
	return stub
}

func (s *supplierStub) received() []recordedRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]recordedRequest(nil), s.requests...)
}

type publishedMessage struct {
	exchange string
	key      string
	envelope *mqp.Envelope
}

// Handle 同步執行，發布也在同一條 goroutine 上，所以這裡不需要 mutex
type publishRecorder struct {
	messages []publishedMessage
	err      error // 非 nil 時每一次發布都失敗，模擬 broker 不通或關機途中的 ctx 取消
}

func (r *publishRecorder) publish(_ context.Context, exchange, key string, body []byte) error {
	var envelope mqp.Envelope
	if err := protojson.Unmarshal(body, &envelope); err != nil {
		return err
	}

	r.messages = append(r.messages, publishedMessage{exchange: exchange, key: key, envelope: &envelope})
	return r.err
}

// ─────────────────────────────────────────────
// 共用組裝與斷言
// ─────────────────────────────────────────────

func newTestWorker(stub *supplierStub, client *http.Client) *Worker {
	return NewWorker(client, config.Config{
		FiatAPIKey:   "fiat-key",
		CryptoAPIKey: "crypto-key",
		FiatURL:      stub.url + "/v6/latest/%s",
		CryptoURL:    stub.url + "/api/v3/simple/price?vs_currencies=%s&ids=%s",
	}, testExchange)
}

// 測試用 client 的上限只是防止測試掛住；真正的 5s 由 main 提供，見 TestSupplierClientBoundsEveryCall
func testClient() *http.Client {
	return &http.Client{Timeout: 5 * time.Second}
}

func currencyPairMessage(t *testing.T, pair *erp.CurrencyPair) rabbitmq.Message {
	t.Helper()

	body, err := protojson.Marshal(pair)
	if err != nil {
		t.Fatalf("組裝 CurrencyPair 訊息失敗: %v", err)
	}

	return rabbitmq.Message{Body: body}
}

// 用 Fatalf 而非 Errorf：後面的斷言會直接索引 received()，數量對不上就繼續下去會是 index out of range
func assertRequestCount(t *testing.T, stub *supplierStub, want int) {
	t.Helper()

	if got := len(stub.received()); got != want {
		t.Fatalf("供應商收到 %d 次請求, 期望 %d 次", got, want)
	}
}

func assertMessageCount(t *testing.T, recorder *publishRecorder, want int) {
	t.Helper()

	if got := len(recorder.messages); got != want {
		t.Fatalf("發布 %d 則訊息, 期望 %d 則: %+v", got, want, recorder.messages)
	}
}

func assertEnvelope(t *testing.T, got publishedMessage, wantKey string, wantType mqp.EnvelopeType) {
	t.Helper()

	if got.exchange != testExchange {
		t.Errorf("exchange = %q, 期望 %q", got.exchange, testExchange)
	}
	if got.key != wantKey {
		t.Errorf("routing key = %q, 期望 %q", got.key, wantKey)
	}
	if got.envelope.Type != wantType {
		t.Errorf("EnvelopeType = %v, 期望 %v", got.envelope.Type, wantType)
	}
}

func decodeRate(t *testing.T, got publishedMessage) *erp.ExchangeRate {
	t.Helper()

	var rate erp.ExchangeRate
	if err := protojson.Unmarshal([]byte(got.envelope.Data), &rate); err != nil {
		t.Fatalf("解析 Envelope.Data 失敗: %v", err)
	}

	return &rate
}

// ─────────────────────────────────────────────
// 批次語意
// ─────────────────────────────────────────────

/*
 * 一則 CurrencyPair 是一次請求，不是一個 counter 一次。
 *
 * exchangerate-api 的 /latest/{base} 回傳整張 conversion_rates 表，單筆邊界會把這個能力丟掉。
 */
func TestHandleFiatFetchesWholeTableInOneRequest(t *testing.T) {
	stub := newSupplierStub(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"result":"success","conversion_rates":{"USD":0.031234567,"JPY":4.987654321}}`))
	})
	recorder := &publishRecorder{}

	worker := newTestWorker(stub, testClient())
	msg := currencyPairMessage(t, &erp.CurrencyPair{
		Base:    erp.Currency_TWD,
		Counter: []erp.Currency{erp.Currency_USD, erp.Currency_JPY},
		Type:    erp.CurrencyType_CURRENCY_TYPE_FIAT,
	})

	if err := worker.Handle(context.Background(), msg, recorder.publish); err != nil {
		t.Fatalf("Handle() error = %v, 期望 nil", err)
	}

	assertRequestCount(t, stub, 1)
	if got := stub.received()[0].header.Get("Authorization"); got != "Bearer fiat-key" {
		t.Errorf("Authorization = %q, 期望 %q", got, "Bearer fiat-key")
	}

	assertMessageCount(t, recorder, 2)
	// 法幣五位小數，與虛擬貨幣的原樣帶走是刻意的差異
	wantRates := map[erp.Currency]string{
		erp.Currency_USD: "0.03123",
		erp.Currency_JPY: "4.98765",
	}
	for _, message := range recorder.messages {
		assertEnvelope(t, message, successRoutingKey, mqp.EnvelopeType_TELEGRAM_SUCCESS_EXCHANGE_RATE)

		rate := decodeRate(t, message)
		if rate.BaseCurrency != erp.Currency_TWD {
			t.Errorf("BaseCurrency = %v, 期望 %v", rate.BaseCurrency, erp.Currency_TWD)
		}
		if want := wantRates[rate.CounterCurrency]; rate.Rate != want {
			t.Errorf("%v 的 Rate = %q, 期望 %q", rate.CounterCurrency, rate.Rate, want)
		}
		delete(wantRates, rate.CounterCurrency)
	}
	if len(wantRates) != 0 {
		t.Errorf("這些 counter 沒有被發布: %v", wantRates)
	}
}

/*
 * CoinGecko 的 ids（被報價的資產）與 vs_currencies（報價幣別）語意不同，是兩張表。
 *
 * USDT 以 usd 報價是供應商限制造成的替代 —— 後果是回報的 BTC/USDT 實際上是 BTC/USD 的價格，
 * 但對外的 CounterCurrency 仍然是排程要的 USDT。
 */
func TestHandleCryptoQuotesUsdtWithUsdPrice(t *testing.T) {
	stub := newSupplierStub(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"bitcoin":{"usd":68123.456789}}`))
	})
	recorder := &publishRecorder{}

	worker := newTestWorker(stub, testClient())
	msg := currencyPairMessage(t, &erp.CurrencyPair{
		Base:    erp.Currency_BTC,
		Counter: []erp.Currency{erp.Currency_USDT},
		Type:    erp.CurrencyType_CURRENCY_TYPE_CRYPTO,
	})

	if err := worker.Handle(context.Background(), msg, recorder.publish); err != nil {
		t.Fatalf("Handle() error = %v, 期望 nil", err)
	}

	assertRequestCount(t, stub, 1)
	query := stub.received()[0].url.Query()
	if got := query.Get("ids"); got != "bitcoin" {
		t.Errorf("ids = %q, 期望 %q", got, "bitcoin")
	}
	if got := query.Get("vs_currencies"); got != "usd" {
		t.Errorf("vs_currencies = %q, 期望 %q（供應商沒有 usdt 報價）", got, "usd")
	}
	if got := stub.received()[0].header.Get("x-cg-demo-api-key"); got != "crypto-key" {
		t.Errorf("x-cg-demo-api-key = %q, 期望 %q", got, "crypto-key")
	}

	assertMessageCount(t, recorder, 1)
	assertEnvelope(t, recorder.messages[0], successRoutingKey, mqp.EnvelopeType_TELEGRAM_SUCCESS_EXCHANGE_RATE)

	rate := decodeRate(t, recorder.messages[0])
	if rate.CounterCurrency != erp.Currency_USDT {
		t.Errorf("CounterCurrency = %v, 期望 %v", rate.CounterCurrency, erp.Currency_USDT)
	}
	// 虛擬貨幣原樣帶走，不截到法幣的五位小數
	if want := "68123.456789"; rate.Rate != want {
		t.Errorf("Rate = %q, 期望 %q", rate.Rate, want)
	}
}

// ─────────────────────────────────────────────
// 部分完成
// ─────────────────────────────────────────────

/*
 * 某個 counter 查不到時，其他照樣送達，而查不到的那個有一則明確的錯誤通知。
 *
 * 整則訊息仍然算成功（回 nil）—— 回 error 會讓已經送出去的匯率連同整則訊息一起被判定失敗。
 */
func TestHandlePartialFailureDeliversTheRestAndReturnsNil(t *testing.T) {
	stub := newSupplierStub(t, func(w http.ResponseWriter, _ *http.Request) {
		// 回應只有 USD，JPY 整個缺席
		_, _ = w.Write([]byte(`{"result":"success","conversion_rates":{"USD":0.031234567}}`))
	})
	recorder := &publishRecorder{}

	worker := newTestWorker(stub, testClient())
	msg := currencyPairMessage(t, &erp.CurrencyPair{
		Base:    erp.Currency_TWD,
		Counter: []erp.Currency{erp.Currency_USD, erp.Currency_JPY},
		Type:    erp.CurrencyType_CURRENCY_TYPE_FIAT,
	})

	if err := worker.Handle(context.Background(), msg, recorder.publish); err != nil {
		t.Fatalf("Handle() error = %v, 期望 nil（部分完成算整則成功）", err)
	}

	assertRequestCount(t, stub, 1)
	assertMessageCount(t, recorder, 2)

	var failures, successes int
	for _, message := range recorder.messages {
		switch message.key {
		case errorRoutingKey:
			failures++
			assertEnvelope(t, message, errorRoutingKey, mqp.EnvelopeType_TELEGRAM_ERROR)
		case successRoutingKey:
			successes++
			assertEnvelope(t, message, successRoutingKey, mqp.EnvelopeType_TELEGRAM_SUCCESS_EXCHANGE_RATE)
			if got := decodeRate(t, message).CounterCurrency; got != erp.Currency_USD {
				t.Errorf("送達的 CounterCurrency = %v, 期望 %v", got, erp.Currency_USD)
			}
		default:
			t.Errorf("非預期的 routing key: %q", message.key)
		}
	}
	if successes != 1 || failures != 1 {
		t.Errorf("成功 %d 則、失敗 %d 則, 期望各 1 則", successes, failures)
	}
}

// ─────────────────────────────────────────────
// 整批失敗
// ─────────────────────────────────────────────

/*
 * 整批失敗時告警與回傳 error 兩件都做：
 * 前者讓使用者知道，後者讓 core 把 span 標為 Error（Grafana 上唯一標記失敗的地方）。
 */
func TestHandleWholeBatchFailureAlarmsAndReturnsError(t *testing.T) {
	stub := newSupplierStub(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"result":"error","error-type":"invalid-key"}`))
	})
	recorder := &publishRecorder{}

	worker := newTestWorker(stub, testClient())
	msg := currencyPairMessage(t, &erp.CurrencyPair{
		Base:    erp.Currency_TWD,
		Counter: []erp.Currency{erp.Currency_USD},
		Type:    erp.CurrencyType_CURRENCY_TYPE_FIAT,
	})

	if err := worker.Handle(context.Background(), msg, recorder.publish); err == nil {
		t.Fatal("Handle() error = nil, 期望非 nil")
	}

	assertRequestCount(t, stub, 1)
	assertMessageCount(t, recorder, 1)
	assertEnvelope(t, recorder.messages[0], errorRoutingKey, mqp.EnvelopeType_TELEGRAM_ERROR)
}

/*
 * crypto 是一個 counter 一次請求，所以第 N 次掛掉時前 N-1 次的結果已經在手上。
 *
 * 那些匯率必須照樣送達：否認一律 requeue=false 且沒有 DLX，整批丟掉就是永久丟掉，
 * 要等到下一次 cron 才會再查一次。
 */
func TestHandleCryptoMidBatchFailureStillDeliversEarlierRates(t *testing.T) {
	// 依 vs_currencies 分流而不是數呼叫次數：handler 跑在自己的 goroutine 上，共用計數器會是 race
	stub := newSupplierStub(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("vs_currencies") == "twd" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"bitcoin":{"usd":68123.456789}}`))
	})
	recorder := &publishRecorder{}

	worker := newTestWorker(stub, testClient())
	msg := currencyPairMessage(t, &erp.CurrencyPair{
		Base:    erp.Currency_BTC,
		Counter: []erp.Currency{erp.Currency_USD, erp.Currency_TWD},
		Type:    erp.CurrencyType_CURRENCY_TYPE_CRYPTO,
	})

	if err := worker.Handle(context.Background(), msg, recorder.publish); err == nil {
		t.Fatal("Handle() error = nil, 期望非 nil")
	}

	assertRequestCount(t, stub, 2)
	assertMessageCount(t, recorder, 2)

	// 第一個 counter 的匯率照樣送達，第二個以整批失敗告警
	assertEnvelope(t, recorder.messages[0], successRoutingKey, mqp.EnvelopeType_TELEGRAM_SUCCESS_EXCHANGE_RATE)
	if got := decodeRate(t, recorder.messages[0]).CounterCurrency; got != erp.Currency_USD {
		t.Errorf("送達的 CounterCurrency = %v, 期望 %v", got, erp.Currency_USD)
	}
	assertEnvelope(t, recorder.messages[1], errorRoutingKey, mqp.EnvelopeType_TELEGRAM_ERROR)
}

// ─────────────────────────────────────────────
// 發布失敗
// ─────────────────────────────────────────────

/*
 * 查得到但一則都發不出去時要回 error。
 *
 * 回 nil 會讓 core 走 Ack，訊息就此從 broker 上消失。關機途中一定會走到這條路：ctx 一取消，
 * PublishWithRetry 裡的 backoff 立刻回 context.Canceled 而不動用重試預算，每一則發布都失敗。
 * core 專門用來保住這種訊息的 ctx 檢查，只在 handler 回傳 error 時才走得到。
 */
func TestHandleReturnsErrorWhenNothingCouldBePublished(t *testing.T) {
	stub := newSupplierStub(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"result":"success","conversion_rates":{"USD":0.031234567}}`))
	})
	recorder := &publishRecorder{err: errors.New("broker unreachable")}

	worker := newTestWorker(stub, testClient())
	msg := currencyPairMessage(t, &erp.CurrencyPair{
		Base:    erp.Currency_TWD,
		Counter: []erp.Currency{erp.Currency_USD},
		Type:    erp.CurrencyType_CURRENCY_TYPE_FIAT,
	})

	if err := worker.Handle(context.Background(), msg, recorder.publish); err == nil {
		t.Fatal("Handle() error = nil, 期望非 nil：回 nil 會讓 core 確認掉一則什麼都沒送出去的訊息")
	}
}

// ─────────────────────────────────────────────
// 供應商知識
// ─────────────────────────────────────────────

/*
 * 排程把 CurrencyType 標錯時，要得到明確的「不支援此幣別」，而不是一次打到錯誤端點的無效請求。
 *
 * 這是那兩張表真正的價值：窮舉支援範圍。少了表，BTC 被標成 FIAT 就會去打 /latest/BTC。
 */
func TestHandleUnsupportedBaseNeverReachesSupplier(t *testing.T) {
	stub := newSupplierStub(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("不支援的 base 幣別不該送出任何請求")
		w.WriteHeader(http.StatusOK)
	})
	recorder := &publishRecorder{}

	worker := newTestWorker(stub, testClient())
	msg := currencyPairMessage(t, &erp.CurrencyPair{
		Base:    erp.Currency_BTC,
		Counter: []erp.Currency{erp.Currency_USD},
		Type:    erp.CurrencyType_CURRENCY_TYPE_FIAT, // 排程標錯了
	})

	err := worker.Handle(context.Background(), msg, recorder.publish)
	if !errors.Is(err, ErrUnsupportedCurrency) {
		t.Fatalf("Handle() error = %v, 期望包含 ErrUnsupportedCurrency", err)
	}

	assertRequestCount(t, stub, 0)
	assertMessageCount(t, recorder, 1)
	assertEnvelope(t, recorder.messages[0], errorRoutingKey, mqp.EnvelopeType_TELEGRAM_ERROR)
}

// 不支援的 counter 只讓那一個 counter 失敗，其餘照樣送達
func TestHandleUnsupportedCounterFailsOnlyThatCounter(t *testing.T) {
	stub := newSupplierStub(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"result":"success","conversion_rates":{"USD":0.031234567}}`))
	})
	recorder := &publishRecorder{}

	worker := newTestWorker(stub, testClient())
	msg := currencyPairMessage(t, &erp.CurrencyPair{
		Base:    erp.Currency_TWD,
		Counter: []erp.Currency{erp.Currency_USD, erp.Currency_BTC},
		Type:    erp.CurrencyType_CURRENCY_TYPE_FIAT,
	})

	if err := worker.Handle(context.Background(), msg, recorder.publish); err != nil {
		t.Fatalf("Handle() error = %v, 期望 nil", err)
	}

	assertRequestCount(t, stub, 1)
	assertMessageCount(t, recorder, 2)
}

// ─────────────────────────────────────────────
// 分派
// ─────────────────────────────────────────────

/*
 * registry 查不到就回 error。
 *
 * 原本是 map 查找之後直接對 nil 介面呼叫方法 —— 一則訊息的分類錯誤會炸掉整個 process。
 */
func TestHandleUnknownCurrencyTypeReturnsErrorWithoutPanic(t *testing.T) {
	stub := newSupplierStub(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("認不得的 CurrencyType 不該送出任何請求")
		w.WriteHeader(http.StatusOK)
	})
	recorder := &publishRecorder{}

	worker := newTestWorker(stub, testClient())
	msg := currencyPairMessage(t, &erp.CurrencyPair{
		Base:    erp.Currency_TWD,
		Counter: []erp.Currency{erp.Currency_USD},
		Type:    erp.CurrencyType(99), // Contract repo 之後多加一個 enum 值就長這樣
	})

	if err := worker.Handle(context.Background(), msg, recorder.publish); err == nil {
		t.Fatal("Handle() error = nil, 期望非 nil")
	}

	assertRequestCount(t, stub, 0)
}

/*
 * 空的 CurrencyPair 不該變成一次無效請求。
 *
 * CurrencyType_FIAT 與 Currency_TWD 都是 enum 值 0，所以 `{}` 是合法的 protojson，
 * 會解析成「查 TWD 的空清單」—— 沒有這道檢查就是打一次供應商、發布零則結果、然後 Ack。
 */
func TestHandleRejectsPairWithoutCounter(t *testing.T) {
	stub := newSupplierStub(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("沒有 counter 的 CurrencyPair 不該送出任何請求")
		w.WriteHeader(http.StatusOK)
	})
	recorder := &publishRecorder{}

	worker := newTestWorker(stub, testClient())
	msg := currencyPairMessage(t, &erp.CurrencyPair{
		Base: erp.Currency_TWD,
		Type: erp.CurrencyType_CURRENCY_TYPE_FIAT,
	})

	if err := worker.Handle(context.Background(), msg, recorder.publish); err == nil {
		t.Fatal("Handle() error = nil, 期望非 nil")
	}

	assertRequestCount(t, stub, 0)
	assertMessageCount(t, recorder, 0)
}

// 訊息無法解析就回 error（丟棄）—— 連 CurrencyPair 都拼不出來時，沒有可以告警的對象
func TestHandleRejectsUnparsableMessage(t *testing.T) {
	stub := newSupplierStub(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("解析不出來的訊息不該送出任何請求")
		w.WriteHeader(http.StatusOK)
	})
	recorder := &publishRecorder{}

	worker := newTestWorker(stub, testClient())

	if err := worker.Handle(context.Background(), rabbitmq.Message{Body: []byte("not a proto")}, recorder.publish); err == nil {
		t.Fatal("Handle() error = nil, 期望非 nil")
	}

	assertRequestCount(t, stub, 0)
	assertMessageCount(t, recorder, 0)
}

// ─────────────────────────────────────────────
// 超時
// ─────────────────────────────────────────────

/*
 * 傳進來的 client 真的夾得住對外呼叫。
 *
 * 現況用 http.NewRequest 而不是 NewRequestWithContext，ctx 從來沒接上 HTTP 呼叫，
 * 所以 timeout 放哪都不會生效。worker 同步化之後，一個掛住的請求會占住一格 prefetch。
 *
 * 這裡用短上限只是為了讓測試跑得完；值是 5s 這件事由 TestSupplierClientBoundsEveryCall 釘住。
 */
func TestHandleIsBoundedByTheInjectedClientTimeout(t *testing.T) {
	release := make(chan struct{})
	stub := newSupplierStub(t, func(w http.ResponseWriter, _ *http.Request) {
		<-release // 收下請求就不說話，直到測試結束
		w.WriteHeader(http.StatusOK)
	})
	// 註冊在 stub 之後：t.Cleanup 是 LIFO，這個要先跑，否則 server.Close() 會卡在還沒返回的 handler 上
	t.Cleanup(func() { close(release) })

	recorder := &publishRecorder{}

	worker := newTestWorker(stub, &http.Client{Timeout: 100 * time.Millisecond})
	msg := currencyPairMessage(t, &erp.CurrencyPair{
		Base:    erp.Currency_TWD,
		Counter: []erp.Currency{erp.Currency_USD},
		Type:    erp.CurrencyType_CURRENCY_TYPE_FIAT,
	})

	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- worker.Handle(context.Background(), msg, recorder.publish) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Handle() error = nil, 期望非 nil")
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Errorf("Handle() 耗時 %v, 期望被 client 的 100ms 上限夾住", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Handle() 沒有被 client 的上限夾住")
	}
}
