package conn

import (
	"errors"

	"github.com/segmentio/kafka-go"
)

func (kafkaConn *Conn) GetTopics() ([]string, error) {
	var topics []string
	if kafkaConn.Dialer == nil {
		return nil, errors.New("dialer not set up")
	}
	err := kafkaConn.Do(func(conn *kafka.Conn) error {

		p := map[string]struct{}{}
		partitions, err := conn.ReadPartitions()
		if err != nil {
			return err
		}

		for _, p2 := range partitions {
			p[p2.Topic] = struct{}{}
		}
		var list []string
		for k := range p {
			list = append(list, k)
		}
		topics = list
		return nil
	})

	return topics, err

}
func (kafkaConn *Conn) DeleteTopics(topics ...string) error {
	if kafkaConn.Dialer == nil {
		return errors.New("dialer not set up")

	}
	return kafkaConn.Do(func(conn *kafka.Conn) error {

		return conn.DeleteTopics(topics...)
	})
}
func (kafkaConn *Conn) CreateTopic(topicConfig *kafka.TopicConfig) error {

	if kafkaConn.Dialer == nil {
		return errors.New("dialer not set up")
	}
	err := kafkaConn.Do(func(conn *kafka.Conn) error {

		exists := false
		partitions, err := conn.ReadPartitions()
		if err != nil {
			return err
		}

		for _, p2 := range partitions {
			if p2.Topic == topicConfig.Topic {
				exists = true
			}
		}

		if !exists {
			err = conn.CreateTopics(*topicConfig)
		}

		return err
	})
	return err
}
