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
	SaveInventoryConsistently(ctx context.Context, inv *domain.Inventory, productID string, availDelta, resDelta int) error
	SaveInventoriesConsistently(ctx context.Context, inv1 *domain.Inventory, inv2 *domain.Inventory) error

	CreateOrder(ctx context.Context, order *domain.Order) error
	SaveOrderItems(ctx context.Context, orderID string, items []domain.OrderItem) error
	GetOrderItems(ctx context.Context, orderID string) ([]domain.OrderItem, error)
	UpdateOrderStatus(ctx context.Context, orderID string, status string, completedAt time.Time) error

	GetLatestVersion(ctx context.Context, productID string, zoneID string) (int64, error)

	IsEventProcessed(ctx context.Context, eventID string) (bool, error)
	MarkEventProcessed(ctx context.Context, eventID string) (error)
}
