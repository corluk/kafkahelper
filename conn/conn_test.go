package conn

import (
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func TestCreateTopic(t *testing.T) {

	dialer := &kafka.Dialer{
		Timeout:   3 * time.Second,
		DualStack: true,
	}
	conn := Conn{
		Dialer:  dialer,
		Brokers: []string{"localhost:9092"},
	}
	err := conn.CreateTopic(&kafka.TopicConfig{
		Topic:             "test-topic-1",
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	assert.Nil(t, err)

}
