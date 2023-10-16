package utils

import (
	"github.com/segmentio/kafka-go"
)

func GetTopics(conn *kafka.Conn) ([]string, error) {
	p := map[string]struct{}{}
	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, err
	}

	for _, p2 := range partitions {
		p[p2.Topic] = struct{}{}
	}
	var list []string
	for k := range p {
		list = append(list, k)
	}
	return list, nil
}
