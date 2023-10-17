package conn

import (
	"context"
	"errors"

	"github.com/segmentio/kafka-go"
)

type Conn struct {
	Dialer  *kafka.Dialer
	Brokers []string
}

func (Conn *Conn) Setup() (*kafka.Conn, error) {
	if Conn.Dialer == nil {
		return nil, errors.New("dialer is not defined")
	}
	if len(Conn.Brokers) < 1 {
		return nil, errors.New("no broker is defined")
	}
	for _, broker := range Conn.Brokers {

		conn, err := Conn.Dialer.DialContext(context.Background(), "tcp", broker)
		if err == nil {

			return conn, nil
		}

	}
	return nil, errors.New("cannot find valid host")
}
func (kafkaConn *Conn) Do(fn func(conn *kafka.Conn) error) error {
	conn, err := kafkaConn.Setup()
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(conn)

}

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

		if exists == false {
			err = conn.CreateTopics(*topicConfig)
		}

		return err
	})
	return err
}
