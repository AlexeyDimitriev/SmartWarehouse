package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"log"

	"smart-warehouse/internal/application/port"
	"smart-warehouse/internal/domain"
)

var (
	ErrInvalidQuantity = errors.New("Quantity must be positive")
	ErrInsufficientStock = errors.New("Insufficient available stock")
	ErrUnknownEventType = errors.New("Unknown event type")
)

type InventoryService struct {
	repo port.InventoryRepository
}

func NewInventoryService(repo port.InventoryRepository) *InventoryService {
	return &InventoryService{repo: repo}
}

func extractProductID(payload map[string]interface{}) string {
    if v, ok := payload["product_id"].(string); ok {
        return v
    }

    if positions, ok := payload["positions"].([]interface{}); ok && len(positions) > 0 {
        if pos, ok := positions[0].(map[string]interface{}); ok {
            if v, ok := pos["product_id"].(string); ok {
                return v
            }
        }
    }
    return ""
}

func extractZoneID(payload map[string]interface{}) string {
    if v, ok := payload["zone_id"].(string); ok {
        return v
    }
    if positions, ok := payload["positions"].([]interface{}); ok && len(positions) > 0 {
        if pos, ok := positions[0].(map[string]interface{}); ok {
            if v, ok := pos["zone_id"].(string); ok {
                return v
            }
        }
    }
    return ""
}

func (s *InventoryService) HandleEvent(ctx context.Context, event *domain.Event) error {
    alreadyProcessed, procErr := s.repo.IsEventProcessed(ctx, event.EventID)
	if procErr != nil {
		return fmt.Errorf("Failed to check is event is processed: %w", procErr)
	}
	if alreadyProcessed {
		log.Printf("Eventd %s has already been processed", event.EventID)
		return nil
	}
	
	productID := extractProductID(event.Payload)
    zoneID := extractZoneID(event.Payload)

    if productID != "" && zoneID != "" {
        latestVer, err := s.repo.GetLatestVersion(ctx, productID, zoneID)
        if err != nil {
            return fmt.Errorf("get latest version: %w", err)
        }

        if event.Payload["version"] != nil {
            eventVer := int64(event.Payload["version"].(float64))
            if eventVer <= latestVer {
                log.Printf("[VERSION] Skipping stale event %s: event_ver=%d, latest_ver=%d", event.EventID, eventVer, latestVer)
                return nil
            }
        }
    }
	
	var err error

	switch event.EventType {
	case domain.ProductReceived:
		err = s.handleProductReceived(ctx, event)
	case domain.ProductShipped:
		err = s.handleProductShipped(ctx, event)
	case domain.ProductMoved:
		err = s.handleProductMoved(ctx, event)
	case domain.ProductReserved:
		err = s.handleProductReserved(ctx, event)
	case domain.ProductReleased:
		err = s.handleProductReleased(ctx, event)
	case domain.InventoryCounted:
		err = s.handleInventoryCounted(ctx, event)
	case domain.OrderCreated:
		err = s.handleOrderCreated(ctx, event)
	case domain.OrderCompleted:
		err = s.handleOrderCompleted(ctx, event)
	default:
		err = fmt.Errorf("%w: %s", ErrUnknownEventType, event.EventType)
	}

	if err != nil {
		return fmt.Errorf("Event %s failed: %w", event.EventID, err)
	}
	return s.repo.MarkEventProcessed(ctx, event.EventID)
}

func (s *InventoryService) handleProductReceived(ctx context.Context, event *domain.Event) error {
	p, err := decodePayload[domain.ProductReceivedPayload](event.Payload)
	if err != nil {
		return err
	}
	if p.Quantity <= 0 {
		return ErrInvalidQuantity
	}

	inv, err := s.getOrCreateInventory(ctx, p.ProductID, p.ZoneID, event.Timestamp)
	if err != nil {
		return err
	}

	inv.Available += p.Quantity
	inv.EventVersion++

	if err := s.repo.SaveInventory(ctx, inv); err != nil {
		return fmt.Errorf("Failed to save inventory: %w", err)
	}

	return s.repo.UpdateProductTotal(ctx, p.ProductID, p.Quantity, 0)
}

func (s *InventoryService) handleProductShipped(ctx context.Context, event *domain.Event) error {
	p, err := decodePayload[domain.ProductShippedPayload](event.Payload)
	if err != nil {
		return err
	}
	if p.Quantity <= 0 {
		return ErrInvalidQuantity
	}

	inv, err := s.repo.GetByProductAndZone(ctx, p.ProductID, p.ZoneID)
	if err != nil {
		return err
	}
	if inv.Available < p.Quantity {
		return ErrInsufficientStock
	}

	inv.Available -= p.Quantity
	inv.LastUpdated = event.Timestamp
	inv.EventVersion++
	if err := s.repo.SaveInventory(ctx, inv); err != nil {
		return fmt.Errorf("Failed to save inventory: %w", err)
	}

	return s.repo.UpdateProductTotal(ctx, p.ProductID, -p.Quantity, 0)
}

func (s *InventoryService) handleProductMoved(ctx context.Context, event *domain.Event) error {
	p, err := decodePayload[domain.ProductMovedPayload](event.Payload)
	if err != nil {
		return err
	}
	if p.Quantity <= 0 {
		return ErrInvalidQuantity
	}

	fromInv, err := s.repo.GetByProductAndZone(ctx, p.ProductID, p.FromZone)
	if err != nil {
		return err
	}
	if fromInv.Available < p.Quantity {
		return ErrInsufficientStock
	}

	fromInv.Available -= p.Quantity
	fromInv.LastUpdated = event.Timestamp
	fromInv.EventVersion++
	if err := s.repo.SaveInventory(ctx, fromInv); err != nil {
		return err
	}

	toInv, err := s.getOrCreateInventory(ctx, p.ProductID, p.ToZone, event.Timestamp)
	if err != nil {
		return err
	}

	toInv.Available += p.Quantity
	toInv.EventVersion++
	return s.repo.SaveInventory(ctx, toInv)
}

func (s *InventoryService) handleProductReserved(ctx context.Context, event *domain.Event) error {
	p, err := decodePayload[domain.ProductReservedPayload](event.Payload)
	if err != nil {
		return err
	}
	if p.Quantity <= 0 {
		return ErrInvalidQuantity
	}

	inv, err := s.repo.GetByProductAndZone(ctx, p.ProductID, p.ZoneID)
	if err != nil {
		return err
	}
	if inv.Available < p.Quantity {
		return ErrInsufficientStock
	}

	inv.Available -= p.Quantity
	inv.Reserved += p.Quantity
	inv.LastUpdated = event.Timestamp
	inv.EventVersion++
	if err := s.repo.SaveInventory(ctx, inv); err != nil {
		return fmt.Errorf("Failed to save inventory: %w", err)
	}

	return s.repo.UpdateProductTotal(ctx, p.ProductID, -p.Quantity, p.Quantity)
}

func (s *InventoryService) handleProductReleased(ctx context.Context, event *domain.Event) error {
	p, err := decodePayload[domain.ProductReleasedPayload](event.Payload)
	if err != nil {
		return err
	}
	if p.Quantity <= 0 {
		return ErrInvalidQuantity
	}

	inv, err := s.repo.GetByProductAndZone(ctx, p.ProductID, p.ZoneID)
	if err != nil {
		return err
	}
	if inv.Reserved < p.Quantity {
		return ErrInsufficientStock
	}

	inv.Reserved -= p.Quantity
	inv.Available += p.Quantity
	inv.LastUpdated = event.Timestamp
	inv.EventVersion++
	if err := s.repo.SaveInventory(ctx, inv); err != nil {
		return fmt.Errorf("Failed to save inventory: %w", err)
	}

	return s.repo.UpdateProductTotal(ctx, p.ProductID, p.Quantity, -p.Quantity)
}

func (s *InventoryService) handleInventoryCounted(ctx context.Context, event *domain.Event) error {
	p, err := decodePayload[domain.InventoryCountedPayload](event.Payload)
	if err != nil {
		return err
	}
	if p.Quantity < 0 {
		return ErrInvalidQuantity
	}

	inv, err := s.getOrCreateInventory(ctx, p.ProductID, p.ZoneID, event.Timestamp)
	if err != nil {
		return err
	}

	oldAvailable := inv.Available
	oldReserved := inv.Reserved

	inv.Available = p.Quantity
	inv.Reserved = 0 // maybe?
	inv.LastUpdated = event.Timestamp
	inv.EventVersion++
	if err := s.repo.SaveInventory(ctx, inv); err != nil {
		return fmt.Errorf("Failed to save inventory: %w", err)
	}

	return s.repo.UpdateProductTotal(ctx, p.ProductID, p.Quantity - oldAvailable, -oldReserved)
}

func (s *InventoryService) handleOrderCreated(ctx context.Context, event *domain.Event) error {
	p, err := decodePayload[domain.OrderCreatedPayload](event.Payload)
	if err != nil {
		return err
	}

	if err := s.repo.CreateOrder(ctx, &domain.Order{
		ID: p.OrderID,
		Status: "CREATED",
		CreatedAt: event.Timestamp,
	}); err != nil {
		return fmt.Errorf("Failed to create order record: %w", err)
	}

	var items []domain.OrderItem
	for _, pos := range p.Positions {
		items = append(items, domain.OrderItem{
			OrderID: p.OrderID,
			ProductID: pos.ProductID,
			ZoneID: pos.ZoneID,
			Quantity: pos.Quantity,
		})
	}
	if err := s.repo.SaveOrderItems(ctx, p.OrderID, items); err != nil {
		return fmt.Errorf("Failed to save order items: %w", err)
	}

	for _, pos := range p.Positions {
		inv, err := s.repo.GetByProductAndZone(ctx, pos.ProductID, pos.ZoneID)
		if err != nil {
			return fmt.Errorf("Failed to get inventory for %s: %w", pos.ProductID, err)
		}
		if inv.Available < pos.Quantity {
			return ErrInsufficientStock
		}

		inv.Available -= pos.Quantity
		inv.Reserved += pos.Quantity
		inv.LastUpdated = event.Timestamp
		inv.EventVersion++
		
		if err := s.repo.SaveInventory(ctx, inv); err != nil {
			return fmt.Errorf("Failed to reserve inventory for %s: %w", pos.ProductID, err)
		}

		if err := s.repo.UpdateProductTotal(ctx, pos.ProductID, -pos.Quantity, pos.Quantity); err != nil {
			return fmt.Errorf("Failed to reserve inventory for %s: %w", pos.ProductID, err)
		}
	}

	return nil
}

func (s *InventoryService) handleOrderCompleted(ctx context.Context, event *domain.Event) error {
	p, err := decodePayload[domain.OrderCompletedPayload](event.Payload)
	if err != nil {
		return err
	}

	items, err := s.repo.GetOrderItems(ctx, p.OrderID)
	if err != nil {
		return fmt.Errorf("Failed to fetch order items for completion: %w", err)
	}

	if err := s.repo.UpdateOrderStatus(ctx, p.OrderID, "COMPLETED", event.Timestamp); err != nil {
		return fmt.Errorf("Failed to update order status: %w", err)
	}

	for _, item := range items {
		inv, err := s.repo.GetByProductAndZone(ctx, item.ProductID, item.ZoneID)
		if err != nil {
			return fmt.Errorf("Failed to get inventory for %s/%s: %w", item.ProductID, item.ZoneID, err)
		}

		if inv.Reserved < item.Quantity {
			return ErrInsufficientStock
		}

		inv.Reserved -= item.Quantity
		inv.LastUpdated = event.Timestamp
		inv.EventVersion++

		if err := s.repo.SaveInventory(ctx, inv); err != nil {
			return fmt.Errorf("Failed to save inventory after completion: %w", err)
		}

		if err := s.repo.UpdateProductTotal(ctx, item.ProductID, item.Quantity, -item.Quantity); err != nil {
			return fmt.Errorf("Failed to save inventory after completion: %w", err)
		}
	}

	return nil
}

func (s *InventoryService) getOrCreateInventory(ctx context.Context, productID, zoneID string, ts time.Time) (*domain.Inventory, error) {
	inv, err := s.repo.GetByProductAndZone(ctx, productID, zoneID)
	if err == nil {
		return inv, nil
	}
	if errors.Is(err, port.ErrNotFound) {
		return &domain.Inventory{
			ProductID: productID,
			ZoneID: zoneID,
			Available: 0,
			Reserved: 0,
			LastUpdated: ts,
			EventVersion: 0,
		}, nil
	}
	return nil, err
}

func decodePayload[T any](payload map[string]interface{}) (*T, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("Marshal payload error: %w", err)
	}
	var t T
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("Unmarshal payload error: %w", err)
	}
	return &t, nil
}
