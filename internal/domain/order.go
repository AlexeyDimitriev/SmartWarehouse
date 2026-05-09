package domain

import (
	"time"
)

type Order struct {
	ID string
	Status string
	CreatedAt time.Time
	CompletedAt time.Time
}

type OrderItem struct {
	OrderID string
	ProductID string
	ZoneID string
	Quantity int
}
