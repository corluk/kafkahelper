package conn

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

// KafkaMagic
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

func (kafkaConn *Conn) WriteJSON(topic string, messages []interface{}) error {
	conn, err := kafkaConn.Setup()
	w := &kafka.Writer{
		Addr:     kafka.TCP(kafkaConn.Brokers...),
		Topic:    "test-topic",
		Balancer: &kafka.LeastBytes{},
		Transport: &kafka.Transport{
			Dial: kafkaConn.Dialer.DialFunc,
			SASL: kafkaConn.Dialer.SASLMechanism,
		},
	}

	writer := kafka.NewConnWith(conn, kafka.ConnConfig{
		Topic: topic,
	})
	if err != nil {
		return err
	}

	var kafkaMessages []kafka.Message

	for _, message := range messages {

		b, err := json.Marshal(message)
		if err == nil {
			kafkaMessages = append(kafkaMessages, kafka.Message{

				Value: b,
			})

		}
	}

	defer writer.Close()
	return w.WriteMessages(context.Background(), kafkaMessages...)

}

type Reader struct {
	Conn   *Conn
	Config *kafka.ReaderConfig
}

func (kafkaReader *Reader) Read(cb func(value interface{}) error) error {
	if kafkaReader.Conn == nil {
		return errors.New("no conn")
	}
	if kafkaReader.Config == nil {
		return errors.New("no config")
	}
	kafkaReader.Config.Dialer = kafkaReader.Conn.Dialer
	kafkaReader.Config.Brokers = kafkaReader.Conn.Brokers

	reader := kafka.NewReader(*kafkaReader.Config)
	for {
		msg, err := reader.ReadMessage(context.Background())
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
