package domain

import (
	"time"
)

type DLQMessage struct {
	OriginalEvent map[string]interface{} `json:"original_event"`
	ErrorReason string `json:"error_reason"`
	ErrorCode string `json:"error_code"`
	FailedAt time.Time `json:"failed_at"`
	KafkaMetadata KafkaMetadata `json:"kafka_metadata"`
}

type KafkaMetadata struct {
	Partition int32 `json:"partition"`
	Offset int64 `json:"offset"`
}
