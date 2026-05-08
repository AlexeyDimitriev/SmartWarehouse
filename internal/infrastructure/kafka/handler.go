package kafka

import (
	"context"
	"log"

	"smart-warehouse/internal/domain"
)

type SimpleHandler struct {

}

func NewSimpleHandler() *SimpleHandler {
	return &SimpleHandler {

	}
}

func (h *SimpleHandler) Handle(ctx context.Context, event *domain.Event) error {
	// to be done
	log.Printf("Event handled: %s - %s", event.EventID, event.EventType)
	return nil
}
