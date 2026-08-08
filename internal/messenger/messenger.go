package messenger

import "context"
import "time"

// StockEvent represents a single stock update received from Kafka.
type StockEvent struct {
	Symbol         string    `json:"symbol"`
	CompanyName    string    `json:"company_name,omitempty"`
	Exchange       string    `json:"exchange"`
	Currency       string    `json:"currency"`
	Price          float64   `json:"price"`
	ChangePercent  float64   `json:"change_percent,omitempty"`
	SentimentScore float64   `json:"sentiment_score,omitempty"`
	Timestamp      time.Time `json:"timestamp"`
}

// Formatter produces formatted text from a single stock event.
type Formatter interface {
	Format(event *StockEvent) string
}

// Publisher posts text to an external service (e.g., GoToSocial).
type Publisher interface {
	Publish(ctx context.Context, text string) error
}
