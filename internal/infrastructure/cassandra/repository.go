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
		SELECT product_id, available, reserved, last_updated
		FROM inventory_by_product 
		WHERE product_id = ?`,
		productID,
	).WithContext(
		ctx,
	).Scan(
		&inv.ProductID,
		&inv.Available,
		&inv.Reserved,
		&inv.LastUpdated,
	)

	if err == gocql.ErrNotFound {
		return nil, port.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("Cassandra query failed: %w", err)
	}
	return &inv, nil
}

func (r *Repository) GetByZone(ctx context.Context, zoneID string) ([]*domain.ZoneInventory, error) {
	iter := r.session.Query(`
		SELECT zone_id, product_id, available_qty, reserved_qty, last_updated
		FROM inventory_by_zone 
		WHERE zone_id = ?`,
		zoneID,
	).WithContext(
		ctx,
	).Iter()
	defer iter.Close()

	var result []*domain.ZoneInventory
	var inv domain.ZoneInventory
	for iter.Scan(&inv.ZoneID, &inv.ProductID, &inv.Available, &inv.Reserved, &inv.LastUpdated) {
		item := inv
		result = append(result, &item)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("Cassandra iteration failed: %w", err)
	}
	return result, nil
}

func (r *Repository) Session() *gocql.Session {
	return r.session
}

func (r *Repository) Close() {
	r.session.Close()
}
