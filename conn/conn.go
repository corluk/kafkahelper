package conn

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

type ConnMode string

const (
	CONTROLLER ConnMode = "controller"
)

type Conn struct {
	Dialer         *kafka.Dialer
	Brokers        []string
	WithController bool
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
			if !Conn.WithController {
				return conn, err
			}
			controller, err := conn.Controller()
			if err != nil {
				continue
			}
			conn.Close()
			conn, err := Conn.Dialer.DialContext(context.Background(), "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))

			if err != nil {
				continue
			}
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
func (kafkaConn *Conn) WriteJSON(topic string, messages []interface{}) (int, error) {
	conn, err := kafkaConn.Setup()
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	var kafkaMessages []kafka.Message
	for _, message := range messages {

		b, err := json.Marshal(message)
		if err == nil {
			kafkaMessages = append(kafkaMessages, kafka.Message{
				Topic: topic,
				Value: b,
			})

		}
	}
	defer conn.Close()
	return conn.WriteMessages(kafkaMessages...)

}

type KafkaReader struct {
	Topic   string
	GroupId string
	Config  kafka.ReaderConfig
	Reader  *kafka.Reader
}

func (kafkaReader *KafkaReader) InitReader(kafkaConn Conn) error {
	if kafkaConn.Dialer == nil {
		return errors.New("dialer is not defined")
	}
	kafkaReader.Config.Dialer = kafkaConn.Dialer
	kafkaReader.Config.Brokers = kafkaConn.Brokers
	kafkaReader.Config.Topic = kafkaReader.Topic
	kafkaReader.Config.GroupID = kafkaReader.GroupId
	kafkaReader.Reader = kafka.NewReader(kafkaReader.Config)
	return nil
}
func (kafkaReader *KafkaReader) Read(cb func(value interface{}) error) {

	for {
		msg, err := kafkaReader.Reader.ReadMessage(context.Background())
		if err == nil {
			var item interface{}
			json.Unmarshal(msg.Value, &item)
			cb(item)
		}
	}
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

func (kafkaConn *Conn) SendMessages(topic string, messages []interface{}) (int, error) {
	if kafkaConn.Dialer == nil {
		return nil, errors.New("dialer not set up")
	}
	var kafkaMessages []kafka.Message
	for _, message := range messages {

		b, err := json.Marshal(message)
		if err == nil {
			kafkaMessages = append(kafkaMessages, kafka.Message{
				Topic: topic,
				Value: b,
			})

		}
	}
	var numOfMessages int
	kafkaConn.Do(func(conn *kafka.Conn) error {

		numOfMessages, err1 := conn.WriteMessages(kafkaMessages...)
		return err1
	})

	return numOfMessages, err
}
