package domain

import "time"

// inventory_by_product_zone
type Inventory struct {
	ProductID string
	ZoneID string
	Available int
	Reserved int 
	LastUpdated time.Time
	EventVersion int64
}

// inventory_by_product
type ProductInventory struct {
	ProductID string
	Available int
	Reserved int 
	LastUpdated time.Time
	EventVersion int64
}

// inventory_by_zone
type ZoneInventory struct {
	ZoneID string
	ProductID string
	Available int
	Reserved int 
	LastUpdated time.Time
	EventVersion int64
}
