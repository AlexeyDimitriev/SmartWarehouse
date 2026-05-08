package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"smart-warehouse/internal/infrastructure/config"
	"smart-warehouse/internal/infrastructure/kafka"
	"smart-warehouse/internal/infrastructure/cassandra"
)

func main() {
	cfg := config.Load()

	log.Println("Initializing Bootstrap...")
	bootstrap, err := cassandra.NewBootstrapClient(
		cfg.CassandraCfg.Hosts,
		cfg.CassandraCfg.Username,
		cfg.CassandraCfg.Password,
	)
	if err != nil {
		log.Printf("Error while creating bootstrap: %s", err.Error())
		os.Exit(1)
	}
	defer bootstrap.Close()

	if err := bootstrap.RunMigrations(cfg.CassandraCfg.Keyspace); err != nil {
		log.Printf("Failed to run migrations: %s", err.Error())
		os.Exit(1)
	}

	log.Println("Initializing Cassandra...")
	cassandraRepo, err := cassandra.NewRepository(
		cfg.CassandraCfg.Hosts,
		cfg.CassandraCfg.Keyspace,
		cfg.CassandraCfg.Username,
		cfg.CassandraCfg.Password,
	)
	if err != nil {
		log.Printf("Error while creating repo: %s", err.Error())
		os.Exit(1)
	}
	defer cassandraRepo.Close()

	handler := kafka.NewSimpleHandler()

	consumer := kafka.NewConsumer(
		kafka.ConsumerConfig{
			Brokers: cfg.KafkaCfg.Brokers,
			Topic: cfg.KafkaCfg.Topic,
			ConsumerGroup: cfg.KafkaCfg.ConsumerGroup,
		},
		handler,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Println("Starting consumer...")
	if err := consumer.Start(ctx); err != nil {
		log.Printf("Failed to start consumer: %v", err.Error())
	}
	log.Println("Consumer started")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Waiting for shutdown signal...")
	<-sigChan
	log.Println("Recieved shutdown signal. Stopping...")

	cancel()

	select {
	case <-consumer.Done():
		log.Println("Consumer stopped.")
	case <-time.After(10 * time.Second):
		log.Println("Consumer shutdown timeout.")
	}

	log.Println("Stopped.")
}
