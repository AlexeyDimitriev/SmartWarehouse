package port

import (
	"context"
	"errors"
	"smart-warehouse/internal/domain"
)

var ErrNotFound = errors.New("Not found")

type EventRepository interface {
	IsProcessed(ctx context.Context, eventID string) (bool, error)
	MarkProcessed(ctx context.Context, eventID string) (error)
}

type InventoryRepository interface {
	GetByProductAndZone(ctx context.Context, productID string, zoneID string) (*domain.Inventory, error)
	GetByProduct(ctx context.Context, productID string) (*domain.ProductInventory, error)
	GetByZone(ctx context.Context, zoneID string) ([]*domain.ZoneInventory, error)
}
