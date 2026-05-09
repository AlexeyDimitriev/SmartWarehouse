package port

import (
	"context"
	"errors"
	"time"

	"smart-warehouse/internal/domain"
)

var ErrNotFound = errors.New("Not found")

type InventoryRepository interface {
	GetByProductAndZone(ctx context.Context, productID string, zoneID string) (*domain.Inventory, error)
	GetByProduct(ctx context.Context, productID string) (*domain.ProductInventory, error)
	GetByZone(ctx context.Context, zoneID string) ([]*domain.ZoneInventory, error)
	SaveInventory(ctx context.Context, inv *domain.Inventory) error
	UpdateProductTotal(ctx context.Context, productID string, availDelta, resDelta int) error
	
	CreateOrder(ctx context.Context, order *domain.Order) error
	SaveOrderItems(ctx context.Context, orderID string, items []domain.OrderItem) error
	GetOrderItems(ctx context.Context, orderID string) ([]domain.OrderItem, error)
	UpdateOrderStatus(ctx context.Context, orderID string, status string, completedAt time.Time) error

	GetLatestVersion(ctx context.Context, productID string, zoneID string) (int64, error)

	IsProcessed(ctx context.Context, eventID string) (bool, error)
	MarkProcessed(ctx context.Context, eventID string) (error)
}
