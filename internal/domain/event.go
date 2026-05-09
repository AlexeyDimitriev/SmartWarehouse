package domain

import (
	"time"
)

type EventType string

const (
	ProductReceived EventType = "PRODUCT_RECEIVED"
	ProductShipped EventType = "PRODUCT_SHIPPED"
	ProductMoved EventType = "PRODUCT_MOVED"
	ProductReserved EventType = "PRODUCT_RESERVED"
	ProductReleased EventType = "PRODUCT_RELEASED"
	InventoryCounted EventType = "INVENTORY_COUNTED"
	OrderCreated EventType = "ORDER_CREATED"
	OrderCompleted EventType = "ORDER_COMPLETED"
)

type Event struct {
	EventID string `json:"event_id"`
	EventType EventType `json:"event_type"`
	Timestamp time.Time `json:"timestamp"`
	Payload map[string]interface{} `json:"payload"`
}

type ProcessedEvent struct {
	EventID string `json:"event_id"`
	EventType string `json:"event_type"`
	Offset int64 `json:"offset"`
	Partition int32 `json:"partition"`
}

type ProductReceivedPayload struct {
	ProductID string `json:"product_id"`
	ZoneID string `json:"zone_id"`
	Quantity int `json:"quantity"`
}

type ProductShippedPayload struct {
	ProductID string `json:"product_id"`
	ZoneID string `json:"zone_id"`
	Quantity int `json:"quantity"`
}

type ProductMovedPayload struct {
	ProductID string `json:"product_id"`
	FromZone string `json:"from_zone"`
	ToZone string `json:"to_zone"`
	Quantity int `json:"quantity"`
}

type ProductReservedPayload struct {
	ProductID string `json:"product_id"`
	ZoneID string `json:"zone_id"`
	Quantity int `json:"quantity"`
}

type ProductReleasedPayload struct {
	ProductID string `json:"product_id"`
	ZoneID string `json:"zone_id"`
	Quantity int `json:"quantity"`
}

type InventoryCountedPayload struct {
	ProductID string `json:"product_id"`
	ZoneID string `json:"zone_id"`
	Quantity int `json:"counted_quantity"`
}

type OrderPositionPayload struct {
	ProductID string `json:"product_id"`
	ZoneID string `json:"zone_id"`
	Quantity int `json:"quantity"`
}

type OrderCreatedPayload struct {
	OrderID string `json:"order_id"`
	Positions []OrderPositionPayload `json:"positions"`
}

type OrderCompletedPayload struct {
 	OrderID string `json:"order_id"`
}
