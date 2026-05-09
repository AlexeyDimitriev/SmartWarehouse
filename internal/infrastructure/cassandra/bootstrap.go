package cassandra

import (
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
)

type BootstrapClient struct {
	session *gocql.Session
}

func NewBootstrapClient(hosts []string, username, password string) (*BootstrapClient, error) {
	var session *gocql.Session
	var err error

	for attempt := 1; attempt <= 10; attempt++ {
		log.Printf("Bootstrap attempt %d/10 to %v...", attempt, hosts)

		cluster := gocql.NewCluster(hosts...)
		cluster.Keyspace = ""
		cluster.Consistency = gocql.One
		cluster.Timeout = 10 * time.Second
		cluster.ConnectTimeout = 10 * time.Second

		if username != "" && password != "" {
			cluster.Authenticator = gocql.PasswordAuthenticator{
				Username: username,
				Password: password,
			}
		}

		session, err = cluster.CreateSession()
		if err == nil {
			log.Println("Bootstrap connected successfully!")
			break
		}

		log.Printf("Bootstrap failed: %v. Retrying in 5s...", err)
		time.Sleep(10 * time.Second)
	}

	if err != nil {
		return nil, fmt.Errorf("Bootstrap failed after 10 attempts: %w", err)
	}

	return &BootstrapClient{session: session}, nil
}

func (b *BootstrapClient) RunMigrations(keyspace string) error {
	log.Printf("Running migrations for keyspace: %s", keyspace)

	queries := []string{
		fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}`, keyspace),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.inventory_by_product_zone (
			product_id TEXT,
			zone_id TEXT,
			available INT,
			reserved INT,
			last_updated TIMESTAMP,
			event_version BIGINT,
			PRIMARY KEY ((product_id), zone_id)
		)`, keyspace),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.inventory_by_product (
			product_id TEXT,
			available INT,
			reserved INT,
			last_updated TIMESTAMP,
			event_version BIGINT,
			PRIMARY KEY (product_id)
		)`, keyspace),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.inventory_by_zone (
			zone_id TEXT,
			product_id TEXT,
			available INT,
			reserved INT,
			last_updated TIMESTAMP,
			event_version BIGINT,
			PRIMARY KEY ((zone_id), product_id)
		)`, keyspace),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.orders (
			order_id TEXT,
			status TEXT,
			created_at TIMESTAMP,
			completed_at TIMESTAMP,
			PRIMARY KEY (order_id)
		)`, keyspace),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.order_items (
			order_id TEXT,
			product_id TEXT,
			zone_id TEXT,
			quantity INT,
			PRIMARY KEY ((order_id), product_id)
		)`, keyspace),
	}

	for i, q := range queries {
		log.Printf("Executing migration %d/%d", i+1, len(queries))
		if err := b.session.Query(q).Exec(); err != nil {
			return fmt.Errorf("Migration %d failed: %w", i+1, err)
		}
	}

	log.Println("Migrations completed successfully")
	return nil
}

func (b *BootstrapClient) Close() {
	if b.session != nil {
		b.session.Close()
	}
}