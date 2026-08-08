package messenger

import (
	"fmt"
)

// StockEventFormatter produces GoToSocial text from a single stock event.
type StockEventFormatter struct{}

var _ Formatter = (*StockEventFormatter)(nil)

func NewStockEventFormatter() *StockEventFormatter {
	return &StockEventFormatter{}
}

func (f *StockEventFormatter) Format(event *StockEvent) string {
	text := fmt.Sprintf("# Stock Update - %s", event.Symbol)

	if event.CompanyName != "" || event.Exchange != "" {
		parts := []string{event.CompanyName, event.Exchange}
		for _, p := range parts {
			if p == "" {
				continue
			}
		}
		text += fmt.Sprintf("\n*%s* (%s/%s)", event.CompanyName, event.Exchange, event.Currency)
	}

	line := fmt.Sprintf("Price: $%.2f", event.Price)
	if event.ChangePercent > 0 {
		line = fmt.Sprintf("%s (+%.1f%%)", line, event.ChangePercent)
	} else if event.ChangePercent < 0 {
		line = fmt.Sprintf("%s (%.1f%%)", line, event.ChangePercent)
	}

	if event.SentimentScore > 0 {
		line = fmt.Sprintf(" | Sentiment: %.2f", event.SentimentScore)
	}

	text += "\n" + line

	return text
}
