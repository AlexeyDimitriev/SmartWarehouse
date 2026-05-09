package cassandra

import (
	"context"
	"fmt"
	"smart-warehouse/internal/application/port"
	"smart-warehouse/internal/domain"
	"time"

	"github.com/gocql/gocql"
)

type Repository struct {
	session *gocql.Session
	keyspace string
}

func NewRepository(hosts []string, keyspace, username, password string) (*Repository, error) {
	var session *gocql.Session
	var err error

	maxAttempts := 10
	for attempt := 0; attempt < maxAttempts; attempt++ {
		cluster := gocql.NewCluster(hosts...)
		cluster.Keyspace = keyspace
		cluster.Consistency = gocql.One
		cluster.Timeout = 10 * time.Second
		
		if username != "" && password != "" {
			cluster.Authenticator = gocql.PasswordAuthenticator{
				Username: username,
				Password: password,
			}
		}

		session, err = cluster.CreateSession()
		if err == nil {
			return &Repository{
				session:  session,
				keyspace: keyspace,
			}, nil
		}

		time.Sleep(10 * time.Second)
	}

	return nil, fmt.Errorf("Failed to create cassandra session: %w", err)
}

func (r *Repository) GetByProductAndZone(ctx context.Context, productID, zoneID string) (*domain.Inventory, error) {
	var inv domain.Inventory
	err := r.session.Query(`
		SELECT product_id, zone_id, available, reserved, last_updated, event_version
		FROM inventory_by_product_zone 
		WHERE product_id = ? AND zone_id = ?`,
		productID,
		zoneID,
	).WithContext(
		ctx,
	).Scan(
		&inv.ProductID,
		&inv.ZoneID,
		&inv.Available,
		&inv.Reserved,
		&inv.LastUpdated,
		&inv.EventVersion,
	)

	if err == gocql.ErrNotFound {
		return nil, port.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("Cassandra query failed: %w", err)
	}
	return &inv, nil
}

func (r *Repository) GetByProduct(ctx context.Context, productID string) (*domain.ProductInventory, error) {
    var inv domain.ProductInventory
    err := r.session.Query(`
        SELECT product_id, available, reserved, last_updated, event_version 
        FROM inventory_by_product WHERE product_id = ?`,
        productID,
    ).WithContext(ctx).Scan(
        &inv.ProductID, &inv.Available, &inv.Reserved, &inv.LastUpdated, &inv.EventVersion,
    )
    if err == gocql.ErrNotFound {
        return nil, port.ErrNotFound
    }
    if err != nil {
        return nil, fmt.Errorf("cassandra query failed: %w", err)
    }
    return &inv, nil
}

func (r *Repository) GetByZone(ctx context.Context, zoneID string) ([]*domain.ZoneInventory, error) {
    iter := r.session.Query(`
        SELECT zone_id, product_id, available, reserved, last_updated, event_version 
        FROM inventory_by_zone WHERE zone_id = ?`,
        zoneID,
    ).WithContext(ctx).Iter()
    defer iter.Close()
    
    var result []*domain.ZoneInventory
    var inv domain.ZoneInventory
    for iter.Scan(&inv.ZoneID, &inv.ProductID, &inv.Available, &inv.Reserved, &inv.LastUpdated, &inv.EventVersion) {
        item := inv
        result = append(result, &item)
    }
    if err := iter.Close(); err != nil {
        return nil, fmt.Errorf("cassandra iteration failed: %w", err)
    }
    return result, nil
}

func (r *Repository) SaveInventory(ctx context.Context, inv *domain.Inventory) error {
    if err := r.session.Query(`
        INSERT INTO inventory_by_product_zone 
        (product_id, zone_id, available, reserved, last_updated, event_version) 
        VALUES (?, ?, ?, ?, ?, ?)`,
        inv.ProductID, inv.ZoneID, inv.Available, inv.Reserved, inv.LastUpdated, inv.EventVersion,
    ).WithContext(ctx).Exec(); err != nil {
        return fmt.Errorf("Failed to save to inventory_by_product_zone: %w", err)
    }

    if err := r.session.Query(`
        INSERT INTO inventory_by_zone 
        (zone_id, product_id, available, reserved, last_updated, event_version) 
        VALUES (?, ?, ?, ?, ?, ?)`,
        inv.ZoneID, inv.ProductID, inv.Available, inv.Reserved, inv.LastUpdated, inv.EventVersion,
    ).WithContext(ctx).Exec(); err != nil {
        return fmt.Errorf("Failed to save to inventory_by_zone: %w", err)
    }

	if err := r.session.Query(`
		INSERT INTO inventory_by_product (product_id, event_version, last_updated)
		VALUES (?, ?, ?)`,
        inv.ProductID, inv.EventVersion, inv.LastUpdated,
    ).WithContext(ctx).Exec(); err != nil {
        return fmt.Errorf("Failed to update event_version in inventory_by_product: %w", err)
    }

    return nil
}

func (r *Repository) UpdateProductTotal(ctx context.Context, productID string, availDelta, resDelta int) error {
    return r.session.Query(`UPDATE inventory_by_product 
        SET available = available + ?, reserved = reserved + ? 
        WHERE product_id = ?`,
        availDelta, resDelta, productID,
    ).WithContext(ctx).Exec()
}

func (r *Repository) CreateOrder(ctx context.Context, order *domain.Order) error {
	return r.session.Query(`INSERT INTO orders (order_id, status, created_at, completed_at)
		VALUES (?, ?, ?, ?)`,
		order.ID,
		order.Status,
		order.CreatedAt,
		nil,
	).WithContext(
		ctx,
	).Exec()
}

func (r *Repository) SaveOrderItems(ctx context.Context, orderID string, items []domain.OrderItem) error {
	batch := r.session.NewBatch(gocql.UnloggedBatch)
	for _, item := range items {
		batch.Query(`
			INSERT INTO order_items (order_id, product_id, zone_id, quantity)
			VALUES (?, ?, ?, ?)`,
			item.OrderID,
			item.ProductID,
			item.ZoneID,
			item.Quantity,
		)
	}
	return r.session.ExecuteBatch(batch)
}

func (r *Repository) GetOrderItems(ctx context.Context, orderID string) ([]domain.OrderItem, error) {
	iter := r.session.Query(`SELECT order_id, product_id, zone_id, quantity
		FROM order_items
		WHERE order_id = ?`,
		orderID,
	).WithContext(
		ctx,
	).Iter()
	defer iter.Close()

	var items []domain.OrderItem
	var item domain.OrderItem
	for iter.Scan(&item.OrderID, &item.ProductID, &item.ZoneID, &item.Quantity) {
		items = append(items, item)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("Failed to iterate order items: %w", err)
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("Order items not found for %s", orderID)
	}
	return items, nil
}

func (r *Repository) UpdateOrderStatus(ctx context.Context, orderID, status string, completedAt time.Time) error {
	return r.session.Query(`UPDATE orders SET status = ?, completed_at = ? WHERE order_id = ?`,
		status,
		completedAt,
		orderID,
	).WithContext(
		ctx,
	).Exec()
}

func (r *Repository) GetLatestVersion(ctx context.Context, productID, zoneID string) (int64, error) {
    var version int64
    err := r.session.Query(`
        SELECT event_version FROM inventory_by_product_zone 
        WHERE product_id = ? AND zone_id = ?`,
        productID, zoneID,
    ).WithContext(ctx).Scan(&version)
    
    if err == gocql.ErrNotFound {
        return 0, nil
    }
    if err != nil {
        return 0, fmt.Errorf("Failed to get latest version: %w", err)
    }
    return version, nil
}

func (r *Repository) Session() *gocql.Session {
	return r.session
}

func (r *Repository) Close() {
	r.session.Close()
}
