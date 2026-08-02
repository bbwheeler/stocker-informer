package messenger

import (
	"math"
	"strings"
	"testing"
	"time"

	tsxhistoryv1 "github.com/example/tsx-history/gen/tsx/v1"
)

const epsilon = 1e-9

func TestTopStocksMessenger(t *testing.T) {
	snapshots := []*tsxhistoryv1.StockSnapshot{
		{Symbol: "A", CompanyName: "Alpha", Exchange: "SET", Currency: "THB", Financials: 0.8, Sentiment: 0.7, Leadership: 0.6, TypeSentiment: 0.5},
		{Symbol: "B", CompanyName: "Beta", Exchange: "SET", Currency: "THB", Financials: 0.9, Sentiment: 0.8, Leadership: 0.7, TypeSentiment: 0.6},
		{Symbol: "C", CompanyName: "Gamma", Exchange: "TSX", Currency: "CAD", Financials: 0.3, Sentiment: 0.2, Leadership: 0.1, TypeSentiment: 0.0},
	}

	m := NewTopStocksMessenger(0.25, 0.25, 0.25, 0.25)

	t.Run("Name", func(t *testing.T) {
		if m.Name() != "top_stocks" {
			t.Errorf("expected name 'top_stocks', got '%s'", m.Name())
		}
	})

	t.Run("MessageType", func(t *testing.T) {
		if m.MessageType() != TopStocksMessageType {
			t.Errorf("expected %q, got %q", TopStocksMessageType, m.MessageType())
		}
	})

	t.Run("scoresAndRanks", func(t *testing.T) {
		data := m.CreateMessage(snapshots, 3)

		if data.Stocks[0].Symbol != "B" {
			t.Errorf("expected rank 1 = B (highest score), got %s", data.Stocks[0].Symbol)
		}
		if data.Stocks[0].Rank != 1 {
			t.Errorf("expected rank 1 for B, got %d", data.Stocks[0].Rank)
		}

		if data.Stocks[len(data.Stocks)-1].Symbol != "C" {
			t.Errorf("expected last stock = C (lowest score), got %s", data.Stocks[len(data.Stocks)-1].Symbol)
		}
	})

	t.Run("allZerosFiltered", func(t *testing.T) {
		zeroSnap := []*tsxhistoryv1.StockSnapshot{
			{Symbol: "D", Financials: 0, Sentiment: 0, Leadership: 0, TypeSentiment: 0},
		}
		data := m.CreateMessage(zeroSnap, 10)
		if len(data.Stocks) != 0 {
			t.Errorf("expected 0 stocks when all zero-score, got %d", len(data.Stocks))
		}
	})

	t.Run("partialZeroIncluded", func(t *testing.T) {
		partialSnap := []*tsxhistoryv1.StockSnapshot{
			{Symbol: "Y", Exchange: "TSX", Currency: "CAD", Financials: 0.5, Sentiment: 0, Leadership: 0, TypeSentiment: 0},
		}
		data := m.CreateMessage(partialSnap, 10)
		if len(data.Stocks) != 1 {
			t.Fatalf("expected 1 stock (partial zero scores included), got %d", len(data.Stocks))
		}
	})

	t.Run("tiebreakingBySymbol", func(t *testing.T) {
		tieSnap := []*tsxhistoryv1.StockSnapshot{
			{Symbol: "Z", CompanyName: "Zulu", Exchange: "SET", Currency: "THB", Financials: 0.5, Sentiment: 0.5, Leadership: 0.5, TypeSentiment: 0.5},
			{Symbol: "A", CompanyName: "Alpha", Exchange: "SET", Currency: "THB", Financials: 0.5, Sentiment: 0.5, Leadership: 0.5, TypeSentiment: 0.5},
		}

		mTie := NewTopStocksMessenger(1.0, 0, 0, 0)
		data := mTie.CreateMessage(tieSnap, 2)

		if data.Stocks[0].Symbol != "A" {
			t.Errorf("expected A first (alphabetical tiebreaking), got %s", data.Stocks[0].Symbol)
		}
		if data.Stocks[1].Symbol != "Z" {
			t.Errorf("expected Z second, got %s", data.Stocks[1].Symbol)
		}
	})

	t.Run("maxResultsLimit", func(t *testing.T) {
		snapshots := make([]*tsxhistoryv1.StockSnapshot, 10)
		for i := range snapshots {
			snapshots[i] = &tsxhistoryv1.StockSnapshot{
				Symbol:        "Stock" + string(rune('A'+i)),
				CompanyName:   "Company" + string(rune('A'+i)),
				Exchange:      "SET",
				Currency:      "THB",
				Financials:    float64(10 - i) * 0.1,
				Sentiment:     float64(i) * 0.1,
				Leadership:    float64(i*2) * 0.1,
				TypeSentiment: float64(len(snapshots) - i),
			}
		}

		data := m.CreateMessage(snapshots, 3)
		if len(data.Stocks) != 10 {
			t.Errorf("expected 10 total stocks, got %d", len(data.Stocks))
		}

		for _, s := range data.Stocks[:3] {
			if s.Rank == 0 {
				t.Errorf("expected rank > 0 for top 3 stock %s", s.Symbol)
			}
		}
		for _, s := range data.Stocks[3:] {
			if s.Rank != 0 {
				t.Errorf("expected rank 0 for unranked %s, got %d", s.Symbol, s.Rank)
			}
		}
	})

	t.Run("correctWeightedScore", func(t *testing.T) {
		scoreSnap := []*tsxhistoryv1.StockSnapshot{
			{Symbol: "X", Exchange: "SET", Currency: "THB", Financials: 1.0, Sentiment: 0.0, Leadership: 0.0, TypeSentiment: 0.0},
		}
		mW := NewTopStocksMessenger(1.0, 0.0, 0.0, 0.0)
		data := mW.CreateMessage(scoreSnap, 5)

		expected := 1.0 * 1.0
		if math.Abs(data.Stocks[0].WeightedScore-expected) > epsilon {
			t.Errorf("expected score %f, got %f", expected, data.Stocks[0].WeightedScore)
		}
	})

	t.Run("customWeights", func(t *testing.T) {
		wSnap := []*tsxhistoryv1.StockSnapshot{
			{Symbol: "X", Exchange: "SET", Currency: "THB", Financials: 0.5, Sentiment: 0.5, Leadership: 0.0, TypeSentiment: 0.0},
		}
		mW := NewTopStocksMessenger(0.7, 0.3, 0.0, 0.0)
		data := mW.CreateMessage(wSnap, 5)

		expected := 0.5*0.7 + 0.5*0.3
		if math.Abs(data.Stocks[0].WeightedScore-expected) > epsilon {
			t.Errorf("expected score %f, got %f", expected, data.Stocks[0].WeightedScore)
		}
	})

	t.Run("emptyInput", func(t *testing.T) {
		data := m.CreateMessage(nil, 10)
		if data == nil || len(data.Stocks) != 0 {
			t.Error("expected empty StockData for nil input")
		}
	})
}

func TestFormatTopStocks(t *testing.T) {
	fetchedAt := time.Date(2024, 7, 15, 10, 30, 0, 0, time.UTC)

	tests := []struct {
		name   string
		data   *StockData
		has    string
		notHas string
	}{
		{
			name: "header",
			data: &StockData{Type: TopStocksMessageType, FetchedAt: fetchedAt, Stocks: []StockInfo{{Symbol: "A", Rank: 1}}},
			has:  "# Top Stocks -",
		},
		{
			name: "companyNameDisplayed",
			data: &StockData{Type: TopStocksMessageType, FetchedAt: time.Now(), Stocks: []StockInfo{{Symbol: "TEST", CompanyName: "Test Corp", Exchange: "SET", Currency: "THB", Rank: 1}}},
			has:  "*Company:* Test Corp",
		},
		{
			name:   "emptyStocksReturnsEmpty",
			data:   &StockData{Type: TopStocksMessageType, FetchedAt: time.Now(), Stocks: []StockInfo{}},
			notHas: "#",
		},
	}

	m := NewTopStocksMessenger(0.25, 0.25, 0.25, 0.25)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := m.Format(tt.data)
			if tt.has != "" && !strings.Contains(result, tt.has) {
				t.Errorf("expected output to contain %q, got:\n%s", tt.has, result)
			}
			if tt.notHas != "" && strings.Contains(result, tt.notHas) {
				t.Errorf("expected output NOT to contain %q, got:\n%s", tt.notHas, result)
			}
		})
	}
}

func TestFormat_truncation(t *testing.T) {
	m := NewTopStocksMessenger(0.25, 0.25, 0.25, 0.25)

	t.Run("longMessageTruncated", func(t *testing.T) {
		var stocks []StockInfo
		for i := range 20 {
			stocks = append(stocks, StockInfo{
				Symbol:        "S" + string(rune('A'+i)),
				CompanyName:   "VeryLongCompanyNameToForceTruncation",
				Exchange:      "SET",
				Currency:      "THB",
				Rank:          i + 1,
				Financials:    0.5,
				Sentiment:     0.5,
				Leadership:    0.5,
				TypeSentiment: 0.5,
			})
		}

		data := &StockData{Type: TopStocksMessageType, FetchedAt: time.Now(), Stocks: stocks}
		result := m.Format(data)

		if len(result) > 500 {
			t.Errorf("expected truncated message <= 500 chars, got %d", len(result))
		}
		if !strings.Contains(result, "(truncated)") {
			t.Error("expected truncation notice in output")
		}
	})

	t.Run("shortMessageNoTruncation", func(t *testing.T) {
		data := &StockData{Type: TopStocksMessageType, FetchedAt: time.Now(), Stocks: []StockInfo{{Symbol: "A", Rank: 1}}}
		result := m.Format(data)

		if strings.Contains(result, "(truncated)") {
			t.Error("expected no truncation for short message")
		}
	})
}
