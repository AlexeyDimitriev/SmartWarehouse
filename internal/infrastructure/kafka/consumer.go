package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"
	"errors"

	"github.com/segmentio/kafka-go"

	"smart-warehouse/internal/domain"
	"smart-warehouse/internal/application/service"
)

type EventHandler interface {
 	Handle(ctx context.Context, event *domain.Event) error
}

type ConsumerConfig struct {
	Brokers []string
	Topic string
	DLQTopic string
	ConsumerGroup string
}

// at-least-once
type Consumer struct {
	reader *kafka.Reader
	dlqWriter *kafka.Writer
	config ConsumerConfig
	handler EventHandler
	done chan struct{}
}

func NewConsumer(cfg ConsumerConfig, handler EventHandler) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.Brokers,
		Topic: cfg.Topic,
		GroupID: cfg.ConsumerGroup,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
		MaxWait: 10 * time.Second,
		CommitInterval: 0, // manually
		StartOffset: kafka.FirstOffset,
	})

	dlqWriter := &kafka.Writer{
		Addr: kafka.TCP(cfg.Brokers...),
		Topic: cfg.DLQTopic,
		Balancer: &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async: false,
	}

	return &Consumer{
		reader: reader,
		dlqWriter: dlqWriter,
		config: cfg,
		handler: handler,
		done: make(chan struct{}),
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	log.Printf(
		"Starting kafka consumer (group=%s, topic=%s, brokers=%v)",
		c.config.ConsumerGroup,
		c.config.Topic,
		c.config.Brokers,
	)

	go c.consume(ctx)
	return nil
}

func (c *Consumer) consume(ctx context.Context) {
	defer close(c.done)

	for {
		select {
			case <-ctx.Done():
				log.Println("Context cancelled, stopping kafka consumer")
				return
			default:
				msg, err := c.reader.FetchMessage(ctx)
				if err != nil {
					log.Printf("Error fetching message to kafka: %v", err)
					time.Sleep(time.Second)
					continue
				}

				c.processMessage(ctx, msg)
		}
	}
}

// at-least-once
func (c *Consumer) processMessage(ctx context.Context, msg kafka.Message) {
	var event domain.Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Printf("ERROR Failed to parse event: %v, offset=%d, partition=%d",
		err, msg.Offset, msg.Partition)
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("ERROR Failed to commit offset: %v", err)
		}
		return
	}

	log.Printf(
		"Processing kafka: event_id=%s, event_type=%s, offset=%d, partition=%d",
		event.EventID,
		event.EventType,
		msg.Offset,
		msg.Partition,
	)

	err := c.handler.Handle(ctx, &event)
	if err != nil {
		log.Printf(
			"ERROR Failed to process event: %v, event_id=%s, offset=%d",
			err,
			event.EventID,
			msg.Offset,
		)

		c.sendToDLQ(ctx, msg, err, c.getErrorCode(err))
		c.reader.CommitMessages(ctx, msg)
		return
	}

	// AT-LEAST-ONCE
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		log.Printf(
			"ERROR Failed to commit offset: %v, event_id=%s, offset=%d",
			err,
			event.EventID,
			msg.Offset,
		)
		return
	}

	log.Printf(
		"EVENT Successfully processed: event_id=%s, event_type=%s, offset=%d, partition=%d",
		event.EventID,
		event.EventType,
		msg.Offset,
		msg.Partition,
	)
}

func (c *Consumer) sendToDLQ(ctx context.Context, msg kafka.Message, err error, code string) {
	var original map[string]interface{}
	_ = json.Unmarshal(msg.Value, &original)

	eventID := "unknown"
	if id, ok := original["event_id"].(string); ok {
		eventID = id
	}

	dlqEntry := domain.DLQMessage{
		OriginalEvent: original,
		ErrorReason: err.Error(),
		ErrorCode: code,
		FailedAt: time.Now(),
		KafkaMetadata: domain.KafkaMetadata{
			Partition: int32(msg.Partition),
			Offset: msg.Offset,
		},
	}

	payload, marshalErr := json.Marshal(dlqEntry)
	if marshalErr != nil {
		log.Printf("Failed to marshal DLQ message: %v", marshalErr)
		return
	}

	kafkaMsg := kafka.Message{
		Key: []byte(eventID),
		Value: payload,
	}

	if writeErr := c.dlqWriter.WriteMessages(ctx, kafkaMsg); writeErr != nil {
		log.Printf("Failed to write to DLQ topic: %v", writeErr)
	} else {
		log.Printf("Event %s successfully sent to DLQ", original["event_id"])
	}
}

func (c *Consumer) getErrorCode(err error) string {
	if errors.Is(err, service.ErrInvalidQuantity) {
		return "VALIDATION_ERROR"
	}
	if errors.Is(err, service.ErrInsufficientStock) {
		return "BUSINESS_RULE_VIOLATION"
	}
	return "PROCESSING_ERROR"
}

func (c *Consumer) Stop() error {
	log.Println("Stopping kafka consumer...")
	if c.dlqWriter != nil {
		c.dlqWriter.Close()
	}
	return c.reader.Close()
}

func (c *Consumer) Done() <-chan struct{} {
	return c.done
}

func (c *Consumer) Stats() kafka.ReaderStats {
	return c.reader.Stats()
}
