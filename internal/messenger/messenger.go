package messenger

import (
	"context"
	"time"

	tsxhistoryv1 "github.com/example/tsx-history/gen/tsx/v1"
)

type MessageType string

const (
	TopStocksMessageType MessageType = "top_stocks"
)

type Message struct {
	Type MessageType
	Text string
}

type StockData struct {
	Type      MessageType
	FetchedAt time.Time
	Stocks    []StockInfo
}

type StockInfo struct {
	Symbol        string
	CompanyName   string
	Exchange      string
	Currency      string
	Financials    float64
	Sentiment     float64
	Leadership    float64
	TypeSentiment float64
	EvaluatedAt   time.Time
	WeightedScore float64
	Rank          int
}

type Messenger interface {
	Name() string
	MessageType() MessageType
	CreateMessage(stocks []*tsxhistoryv1.StockSnapshot, maxResults int) *StockData
	Format(data *StockData) string
}

type Publisher interface {
	Publish(ctx context.Context, msg Message) error
}
