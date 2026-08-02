package messenger

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	tsxhistoryv1 "github.com/example/tsx-history/gen/tsx/v1"
)

type TopStocksMessenger struct {
	wFinancials    float64
	wSentiment     float64
	wLeadership    float64
	wTypeSentiment float64
}

func NewTopStocksMessenger(wF, wS, wL, wT float64) *TopStocksMessenger {
	return &TopStocksMessenger{
		wFinancials:    wF,
		wSentiment:     wS,
		wLeadership:    wL,
		wTypeSentiment: wT,
	}
}

func (m *TopStocksMessenger) Name() string                          { return "top_stocks" }
func (m *TopStocksMessenger) MessageType() MessageType             { return TopStocksMessageType }

type scoredSnapshot struct {
	snapshot *tsxhistoryv1.StockSnapshot
	score    float64
}

func (m *TopStocksMessenger) CreateMessage(stocks []*tsxhistoryv1.StockSnapshot, maxResults int) *StockData {
	var scored []scoredSnapshot
	for _, s := range stocks {
		fin := s.GetFinancials()
		sent := s.GetSentiment()
		lead := s.GetLeadership()
		types := s.GetTypeSentiment()

		if fin == 0 && sent == 0 && lead == 0 && types == 0 {
			continue
		}

		score := math.Round((fin*m.wFinancials+sent*m.wSentiment+lead*m.wLeadership+types*m.wTypeSentiment)*1e4) / 1e4
		scored = append(scored, scoredSnapshot{s, score})
	}

	if len(scored) == 0 {
		return &StockData{Type: TopStocksMessageType, FetchedAt: time.Now()}
	}

	// Sort by score descending, then by symbol ascending for tiebreaking
	sort.SliceStable(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score
		}
		return scored[i].snapshot.GetSymbol() < scored[j].snapshot.GetSymbol()
	})

	limit := maxResults
	if limit > len(scored) {
		limit = len(scored)
	}

	stocksOut := make([]StockInfo, 0, len(scored))
	for idx, s := range scored {
		info := m.snapshotToInfo(s.snapshot)
		info.WeightedScore = s.score
		if idx < limit {
			info.Rank = idx + 1
		} else {
			info.Rank = 0
		}
		stocksOut = append(stocksOut, info)
	}

	return &StockData{Type: TopStocksMessageType, FetchedAt: time.Now(), Stocks: stocksOut}
}

func (m *TopStocksMessenger) snapshotToInfo(s *tsxhistoryv1.StockSnapshot) StockInfo {
	var ev time.Time
	ea := s.GetEvaluatedAt()
	if ea != nil {
		ev = ea.AsTime()
	}
	return StockInfo{
		Symbol:        s.GetSymbol(),
		CompanyName:   s.GetCompanyName(),
		Exchange:      s.GetExchange(),
		Currency:      s.GetCurrency(),
		Financials:    s.GetFinancials(),
		Sentiment:     s.GetSentiment(),
		Leadership:    s.GetLeadership(),
		TypeSentiment: s.GetTypeSentiment(),
		EvaluatedAt:   ev,
	}
}

func (m *TopStocksMessenger) Format(data *StockData) string {
	if len(data.Stocks) == 0 {
		return ""
	}

	var lines []string
	lines = append(lines, fmt.Sprintf("# Top Stocks - %s", data.FetchedAt.Format(time.RFC1123)))

	for _, s := range data.Stocks {
		lines = append(lines, "")
		if s.Rank > 0 {
			lines = append(lines, fmt.Sprintf("#%d *%s*", s.Rank, s.Symbol))
		} else {
			lines = append(lines, fmt.Sprintf("*%s*", s.Symbol))
		}
		if s.CompanyName != "" {
			lines = append(lines, fmt.Sprintf("  *Company:* %s", s.CompanyName))
		}
		lines = append(lines, fmt.Sprintf("  *Exchange:* %s/%s", s.Exchange, s.Currency))
		if s.Rank > 0 {
			lines = append(lines, fmt.Sprintf("  *Weighted Score:* %.4f", s.WeightedScore))
		} else {
			lines = append(lines, "  *(not ranked)*")
		}
		lines = append(lines, fmt.Sprintf("  Financials: %f  Sentiment: %f", s.Financials, s.Sentiment))
		lines = append(lines, fmt.Sprintf("  Leadership: %f  TypeSentiment: %f", s.Leadership, s.TypeSentiment))
	}

	msg := strings.Join(lines, "\n")
	if len(msg) > 500 {
		msg = truncateToFit(msg)
	}

	return msg
}

func truncateToFit(s string) string {
	result := s[:500]
	lastBlank := strings.LastIndex(result, "\n\n")
	truncStart := 500 - int(float64(500)*0.3)
	if lastBlank > truncStart {
		return strings.TrimRight(result[:lastBlank+2], "\n ") + " (truncated)"
	}

	lastNewline := strings.LastIndex(result, "\n")
	if lastNewline > 0 {
		return strings.TrimSuffix(result[:lastNewline+1], "\r\n") + " (truncated)"
	}

	return result + " (truncated"
}
