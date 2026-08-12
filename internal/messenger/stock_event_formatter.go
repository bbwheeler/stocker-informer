package messenger

import (
	"fmt"
)

const maxGotosocialTextLength = 500

// StockEventFormatter produces GoToSocial text from a single stock event.
type StockEventFormatter struct{}

var _ Formatter = (*StockEventFormatter)(nil)

func NewStockEventFormatter() *StockEventFormatter {
	return &StockEventFormatter{}
}

func (f *StockEventFormatter) Format(event *StockEvent) string {
	text := fmt.Sprintf("# Stock Update - %s", event.Symbol)

	if event.CompanyName != "" || event.Exchange != "" {
		text += fmt.Sprintf("\n*%s* (%s/%s)", event.CompanyName, event.Exchange, event.Currency)
	}

	line := fmt.Sprintf("Price: $%.2f (+%.1f%%)", event.Price, event.ChangePercent)

	if event.SentimentScore > 0 {
		line = fmt.Sprintf("%s | Sentiment: %.2f", line, event.SentimentScore)
	}

	text += "\n" + line

	return truncateAtUTF8Runes(text, maxGotosocialTextLength)
}

func truncateAtUTF8Runes(s string, max int) string {
	if len(s) <= max {
		return s
	}
	runes := []rune(s)
	if max < 3 {
		return string(runes[:max]) + "…"
	}
	return string(runes[:max-1]) + "…"
}
