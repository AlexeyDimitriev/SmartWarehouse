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
)

func main() {
	cfg := config.Load()

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
