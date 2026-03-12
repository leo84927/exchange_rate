package test

import (
	"exchange_rate/handle"
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

	os.Exit(m.Run())
}

func TestGetExchangeRate_Success(t *testing.T) {
	exchangeRate, err := handle.GetExchangeRate(t.Context(), "USD", "TWD")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if exchangeRate.BaseCurrency == "USD" || exchangeRate.CounterCurrency == "TWD" || exchangeRate.Rate == "" {
		t.Fatal("GetExchangeRate failed")
	}
}
