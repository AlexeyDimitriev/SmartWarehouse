package config

import (
	"os"
	"strings"
)

type Config struct {
	KafkaCfg KafkaConfig
	CassandraCfg CassandraConfig
}

type KafkaConfig struct {
	Brokers []string
	Topic string
	ConsumerGroup string
}

type CassandraConfig struct {
	Hosts []string
	Keyspace string
	Username string
	Password string
}

func Load() *Config {
	return &Config{
		KafkaCfg: *LoadKafkaConfig(),
		CassandraCfg: *LoadCassandraConfig(),
	}
}

func LoadKafkaConfig() *KafkaConfig {
	return &KafkaConfig{
		Brokers: getEnvStringArr("KAFKA_BROKERS", []string{"localhost:9092"}),
		Topic: getEnvString("KAFKA_TOPIC", "warehouse-events"),
		ConsumerGroup: getEnvString("KAFKA_CONSUMER_GROUP", "warehouse-state-consumer"),
	}
}

func LoadCassandraConfig() *CassandraConfig {
	return &CassandraConfig{
		Hosts: getEnvStringArr("CASSANDRA_HOSTS", []string{"localhost"}),
		Keyspace: getEnvString("CASSANDRA_KEYSPACE", "warehouse"),
		Username: getEnvString("CASSANDRA_USERNAME", "cassandra"),
		Password: getEnvString("CASSANDRA_PASSWORD", "casssandra"),
	}
}

func getEnvString(key string, defaultVal string) string {
	got := os.Getenv(key)
	if got != "" {
		return got
	}
	return defaultVal
}

func getEnvStringArr(key string, defaultVal []string) []string {
	got := os.Getenv(key)
	if parts := strings.Split(got, ","); len(parts) != 0 && parts[0] != "" {
		res := make([]string, len(parts))
		copy(res, parts)
		return res
	}
	return defaultVal
}
