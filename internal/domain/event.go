package domain

import (
	"time"
)

type Event struct {
	EventID string `json:"event_id"`
	EventType string `json:"event_type"`
	Timestamp time.Time `json:"timestamp"`
	Payload map[string]interface{} `json:"payload"`
}

type ProcessedEvent struct {
	EventID string `json:"event_id"`
	EventType string `json:"event_type"`
	Offset int64 `json:"offset"`
	Partition int32 `json:"partition"`
}
