package utils

import (
	"github.com/segmentio/kafka-go"
)

func GetTopics(conn *kafka.Conn) (map[string]{},error){

	conn.ReadPartitions()
}
