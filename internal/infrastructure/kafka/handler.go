package kafka

import (
	"context"
	"log"

	"smart-warehouse/internal/domain"
	"smart-warehouse/internal/application/service"
)

type ServiceHandler struct {
	service *service.InventoryService
}

func NewServiceHandler(service *service.InventoryService) *ServiceHandler {
	return &ServiceHandler {
		service: service,
	}
}

func (h *ServiceHandler) Handle(ctx context.Context, event *domain.Event) error {
	log.Printf("Event is processing: %s - %s", event.EventID, event.EventType)

	if err := h.service.HandleEvent(ctx, event); err != nil {
		log.Printf("Error while handling event %s: %s", event.EventID, err.Error())
	}
	log.Printf("Event %s handled successfully", event.EventID)
	return nil
}
