package port

import (
	"errors"
	"context"
)

var ErrNotFound = errors.New("Not found")

type EventRepository interface {
	IsProcessed(ctx context.Context, eventID string) (bool, error)
	MarkProcessed(ctx context.Context, eventID string) (error)
}

type InventoryRepository interface {
	
}
