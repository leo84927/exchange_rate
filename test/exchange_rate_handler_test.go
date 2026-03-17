package test

import (
	"log"
	"os"
	"testing"

	"github.com/joho/godotenv"
)

func TestMain(m *testing.M) {
	// 在所有測試執行前先載入 .env
	if err := godotenv.Load("../.env"); err != nil {
		log.Println("No .env file found, using system env")
	}

	log.Println("Load .env finish")
	os.Exit(m.Run())
}

// func TestGetExchangeRate_Success(t *testing.T) {
// 	log.Println("token ", os.Getenv("EXCHANGE_RATE_API_KEY"))
// 	exchangeRate, err := handle.GetExchangeRate(t.Context(), "USD", "TWD")
// 	if err != nil {
// 		t.Fatalf("expected no error, got: %v", err)
// 	}
// 	if exchangeRate.BaseCurrency == "USD" || exchangeRate.CounterCurrency == "TWD" || exchangeRate.Rate == "" {
// 		t.Fatal("GetExchangeRate failed")
// 	}
// }
