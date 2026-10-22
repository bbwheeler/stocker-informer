package messenger

import "context"

// Publisher posts text to an external service (e.g., GoToSocial).
type Publisher interface {
	Publish(ctx context.Context, text string) error
}
